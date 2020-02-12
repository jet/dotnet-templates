module Fc.Location.Epoch

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] CategoryId = "LocationEpoch"
    let (|For|) (locationId, epochId) = FsCodec.StreamName.compose CategoryId [LocationId.toString locationId; LocationEpochId.toString epochId]

    type CarriedForward = { initial : int }
    type Delta = { delta : int; transaction : InventoryTransactionId }
    type Value = { value : int; transaction : InventoryTransactionId }
    type Event =
        | CarriedForward of CarriedForward
        | Closed
        | Added of Delta
        | Removed of Delta
        | Reset of Value
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type Balance = int
    type OpenState = { count : int; value : Balance }
    type State = Initial | Open of OpenState | Closed of Balance
    let initial = Initial
    let evolve state event =
        match event, state with
        | Events.CarriedForward e, Initial -> Open { count = 0; value = e.initial }
        | Events.Added e, Open bal -> Open { count = bal.count + 1; value = bal.value + e.delta }
        | Events.Removed e, Open bal -> Open { count = bal.count + 1; value = bal.value - e.delta }
        | Events.Reset e, Open bal -> Open { count = bal.count + 1; value = e.value }
        | Events.Closed, Open { value = bal } -> Closed bal
        | Events.CarriedForward _, (Open _|Closed _ as x) -> failwithf "CarriedForward : Unexpected %A" x
        | (Events.Added _|Events.Removed _|Events.Reset _|Events.Closed) as e, (Initial|Closed _ as s) -> failwithf "Unexpected %A when %A" e s
    let fold = Seq.fold evolve

/// Holds events accumulated from a series of decisions while also evolving the presented `state` to reflect the pended events
type private Accumulator() =
    let acc = ResizeArray()
    member __.Ingest state : 'res * Events.Event list -> 'res * Fold.State = function
        | res, [] ->                   res, state
        | res, [e] -> acc.Add e;       res, Fold.evolve state e
        | res, xs ->  acc.AddRange xs; res, Fold.fold state (Seq.ofList xs)
    member __.Accumulated = List.ofSeq acc

type Result<'t> = { balance : Fold.Balance; result : 't option; isOpen : bool }

let sync (balanceCarriedForward : Fold.Balance option) (decide : (Fold.Balance -> 't*Events.Event list)) shouldClose state : Result<'t>*Events.Event list =
    let acc = Accumulator()
    // We require a CarriedForward event at the start of any Epoch's event stream
    let (), state =
        acc.Ingest state <|
            match state with
            | Fold.Initial -> (), [Events.CarriedForward { initial = Option.get balanceCarriedForward }]
            | Fold.Open _ | Fold.Closed _ -> (), []
    // Run, unless we determine we're in Closed state
    let result, state =
        acc.Ingest state <|
            match state with
            | Fold.Initial -> failwith "We've just guaranteed not Initial"
            | Fold.Open { value = bal } -> let r,es = decide bal in Some r,es
            | Fold.Closed _ -> None, []
    // Finally (iff we're `Open`, have run a `decide` and `shouldClose`), we generate a Closed event
    let (balance, isOpen), _ =
        acc.Ingest state <|
            match state with
            | Fold.Initial -> failwith "Can't be Initial"
            | Fold.Open ({ value = bal } as openState) when shouldClose openState -> (bal, false), [Events.Closed]
            | Fold.Open { value = bal } -> (bal, true), []
            | Fold.Closed bal -> (bal, false), []
    { balance = balance; result = result; isOpen = isOpen }, acc.Accumulated

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

    member __.Sync<'R>(locationId, epochId, prevEpochBalanceCarriedForward, decide, shouldClose) : Async<Result<'R>> =
        let stream = resolve (locationId, epochId)
        stream.Transact(sync prevEpochBalanceCarriedForward decide shouldClose)

let create resolve maxAttempts = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = maxAttempts)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, AccessStrategy.Unoptimized).Resolve
    let createService (context,cache,maxAttempts) =
        create (resolve (context,cache)) maxAttempts