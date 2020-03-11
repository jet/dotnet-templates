/// Manages Stock adjustments and deltas for a given Location
/// Provides for controlled opening and closing of an epoch, carrying forward incoming balances when a given Epoch reaches a 'full' state
/// See Location.Service for the logic that allows competing readers/writers to co-operate in bringing this about
module Fc.Location.Epoch

let [<Literal>] Category = "LocationEpoch"
let streamName (locationId, epochId) = FsCodec.StreamName.compose Category [LocationId.toString locationId; LocationEpochId.toString epochId]

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type CarriedForward =   { initial : int; recentTransactions : InventoryTransactionId[] }
    type Event =
        | CarriedForward of CarriedForward
        | Added          of {| delta : int; id : InventoryTransactionId |}
        | Removed        of {| delta : int; id : InventoryTransactionId |}
        | Reset          of {| value : int; id : InventoryTransactionId |}
        | Closed
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State =
        | Initial
        | Open   of Record list    // reverse order, i.e. most recent first
        | Closed of Record list    // trimmed
    and Record =
        | Init of Events.CarriedForward
        | Step of Step
    and Step = { balance : Balance; id : InventoryTransactionId }
    and Balance = int
    let initial = Initial
    let (|Current|) = function
        | (Init { initial = bal } | Step { balance = bal }) :: _ -> bal
        | [] -> failwith "Cannot transact when no CarriedForward"
    let evolve state event =
        match event, state with
        | Events.CarriedForward e, Initial                   -> Open [Init e]
        | Events.Added e,          Open (Current cur as log) -> Open (Step { id = e.id ; balance = cur + e.delta } :: log)
        | Events.Removed e,        Open (Current cur as log) -> Open (Step { id = e.id ; balance = cur - e.delta } :: log)
        | Events.Reset e,          Open log                  -> Open (Step { id = e.id ; balance = e.value       } :: log)
        | Events.Closed,           Open log                  -> Closed log
        | Events.CarriedForward _, (Open _ | Closed _ as x)  -> failwithf "CarriedForward : Unexpected %A" x
        | (Events.Added _ | Events.Removed _ | Events.Reset _ | Events.Closed) as e, (Initial | Closed _ as s) ->
                                                                failwithf "Unexpected %A when %A" e s
    let fold = Seq.fold evolve

/// Holds events accumulated from a series of decisions while also evolving the presented `state` to reflect the pended events
type private Accumulator() =
    let acc = ResizeArray()
    member __.Ingest state : 'res * Events.Event list -> 'res * Fold.State = function
        | res, [] ->                         res, state
        | res, [e] -> acc.Add e;             res, Fold.evolve state e
        | res, xs ->  acc.AddRange xs;       res, Fold.fold state (Seq.ofList xs)
    member __.Accumulated = List.ofSeq acc

type Result<'t> = { history : Fold.Record list; result : 't option; isOpen : bool }

let sync (carriedForward : Events.CarriedForward option)
    (decide : Fold.State -> 't * Events.Event list)
    shouldClose
    state
    : Result<'t> * Events.Event list =

    let acc = Accumulator()
    // 1. Guarantee a CarriedForward event at the start of any Epoch's event stream
    let (), state =
        acc.Ingest state <|
            match state with
            | Fold.Initial ->                (), [Events.CarriedForward (Option.get carriedForward )]
            | Fold.Open _ | Fold.Closed _ -> (), []
    // 2. Transact (unless we determine we're in Closed state)
    let result, state =
        acc.Ingest state <|
            match state with
            | Fold.Initial ->                failwith "We've just guaranteed not Initial"
            | Fold.Open history ->           let r, es = decide state in Some r, es
            | Fold.Closed _ ->               None, []
    // 3. Finally (iff we're `Open`, have run a `decide` and `shouldClose`), we generate a Closed event
    let (history, isOpen), _ =
        acc.Ingest state <|
            match state with
            | Fold.Initial ->                failwith "Can't be Initial"
            | Fold.Open history ->
                if shouldClose history then  (history, false), [Events.Closed]
                else                         (history, true),  []
            | Fold.Closed history ->         (history, false), []
    { history = history; result = result; isOpen = isOpen }, acc.Accumulated

type DupCheckResult = NotDuplicate | IdempotentInsert of Fold.Balance | DupCarriedForward
let private tryFindDup transactionId (history : Fold.Record list) =
    let tryMatch : Fold.Record -> Fold.Balance option option = function
        | Fold.Step { balance = bal; id = id } when id = transactionId -> Some (Some bal)
        | Fold.Init { recentTransactions = prevs } when prevs |> Array.contains transactionId -> Some None
        | _ -> None
    match history |> Seq.tryPick tryMatch with
    | None -> NotDuplicate
    | Some None -> DupCarriedForward
    | Some (Some bal) -> IdempotentInsert bal

type Command =
    | Reset  of value : int
    | Add    of delta : int
    | Remove of delta : int

type Result = Denied | Accepted of Fold.Balance | DupFromPreviousEpoch

let decide transactionId command (state: Fold.State) =
    match state with
    | Fold.Closed _ | Fold.Initial -> failwithf "Cannot apply in state %A" state
    | Fold.Open (Fold.Current cur as history) ->

    match tryFindDup transactionId history with
    | IdempotentInsert bal    -> Accepted bal,         []
    | DupCarriedForward       -> DupFromPreviousEpoch, []
    | NotDuplicate ->

    let accepted, events =
        match command with
        | Reset  value -> true, [Events.Reset   {| value = value; id = transactionId |}]
        | Add    delta -> true, [Events.Added   {| delta = delta; id = transactionId |}]
        | Remove delta when delta > cur -> false, []
        | Remove delta -> true, [Events.Removed {| delta = delta; id = transactionId |}]
    match Fold.fold state events with
    | Fold.Open (Fold.Current cur) -> (if accepted then Accepted cur else Denied), events
    | s -> failwithf "Unexpected state %A" s

type Service internal (resolve : LocationId * LocationEpochId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Sync<'R>(locationId, epochId, prevEpochBalanceCarriedForward, decide, shouldClose) : Async<Result<'R>> =
        let stream = resolve (locationId, epochId)
        stream.Transact(sync prevEpochBalanceCarriedForward decide shouldClose)

let create resolve maxAttempts =
    let resolve locationId =
        let stream = resolve (streamName locationId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = maxAttempts)
    Service(resolve)

module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.Unoptimized
    let create (context, cache, maxAttempts) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve maxAttempts
