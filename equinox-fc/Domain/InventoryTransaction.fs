/// Process Manager used to:
/// - Coordinate competing attempts to transfer quantities from stock; if balance is 3 one of contesting requests to remove 2 or 3 items must reach `Failed`
/// - maintain rolling balance of stock levels per Location
/// - while recording any transfers or adjustment in an overall Inventory record
/// The Process is driven by two actors:
/// 1) The 'happy path', where a given actor is executing the known steps of the command flow
///    In the normal case, such an actor will bring the flow to a terminal state (Completed or Failed)
/// 2) A watchdog-projector, which reacts to observed events in this Category by stepping in to complete in-flight requests that have stalled
///    This represents the case where a 'happy path' actor died, or experienced another impediment on the path.
module Fc.InventoryTransaction

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] CategoryId = "InventoryTransaction"
    let (|For|) transactionId = FsCodec.StreamName.create CategoryId (InventoryTransactionId.toString transactionId)

    type AdjustmentRequested = { location : LocationId; quantity : int }
    type TransferRequested = { source : LocationId; destination : LocationId; quantity : int }
    type Removed = { balance : int }
    type Added = { balance : int }

    type Event =
        (* Stock Adjustment Flow *)
        | AdjustmentRequested of AdjustmentRequested
        | Adjusted

        (* Stock Transfer Flow *)
        | TransferRequested of TransferRequested
        | Failed // terminal
        | Removed of Removed
        | Added of Added

        (* Successful completion *)
        | Completed // terminal
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State =
        | Initial
        | Running of RunningState
        | Completed of TerminalState
    and RunningState =
        | Adjust of Events.AdjustmentRequested
        | Transfer of TransferState
    and TransferState =
        | Requested of Events.TransferRequested
        | Failed of Events.TransferRequested
        | Adding of Removed
        | Added of Added
    and TerminalState =
        | Adjusted of Events.AdjustmentRequested
        | Transferred of Added
        | TransferFailed of Events.TransferRequested
    and AdjustState = Events.AdjustmentRequested
    and Removed = { request : Events.TransferRequested; removed : Events.Removed }
    and Added = { request : Events.TransferRequested; removed : Events.Removed; added : Events.Added }
    let initial = Initial
    let evolve state event =
        match state, event with
        (* Adjustment Process *)
        | Initial, Events.AdjustmentRequested e ->
            Running (Adjust e)
        | Running (Adjust s), Events.Adjusted ->
            Completed (Adjusted s)

        (* Transfer Process *)
        | Initial, Events.TransferRequested e ->
            Running (Transfer (Requested e))

        | Running (Transfer (Requested s)), Events.Failed ->
            Completed (TransferFailed s)

        | Running (Transfer (Requested s)), Events.Removed e as ee ->
            Running (Transfer (Adding { request = s; removed = e }))
        | Running (Transfer (Adding s)), Events.Added e as ee ->
            Running (Transfer (Added { request = s.request; removed = s.removed; added = e  }))
        | Running (Transfer (Added s)), Events.Completed as ee ->
            Completed (Transferred s)

        (* Any disallowed state changes represent gaps in the model, so we fail fast *)
        | state, event -> failwithf "Unexpected %A when %A" event state
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

type Command =
    | Adjust of Events.AdjustmentRequested
    | Transfer of Events.TransferRequested

type Result = { complete : bool;  }

/// Holds events accumulated from a series of decisions while also evolving the presented `state` to reflect the pended events
type private Accumulator() =
    let acc = ResizeArray()
    member __.Ingest(state) : 'res * Events.Event list -> 'res * Fold.State = function
        | res, [] ->                   res, state
        | res, [e] -> acc.Add e;       res, Fold.evolve state e
        | res, xs ->  acc.AddRange xs; res, Fold.fold state (Seq.ofList xs)
    member __.Accumulated = List.ofSeq acc

type Update =
    | AdjustmentCompleted

let decide command updates (state : Fold.State) : Result * Events.Event list =

    let acc = Accumulator()
    let started, state =
        acc.Ingest state <|
            match state, command with
            | Fold.Initial, Adjust e -> true, [ Events.AdjustmentRequested e ]
            | Fold.Initial, Transfer e -> true, [ Events.TransferRequested e ]

            // TOCONSIDER validate conflicts

            | _ -> false, []

    { complete = false }, acc.Accumulated

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For streamId) = Equinox.Stream<Events.Event,Fold.State>(log, resolve streamId, maxAttempts)

    member __.IngestShipped(inventoryId, transactionId, command, updates) : Async<Result> =
        let stream = resolve transactionId
        stream.Transact(decide command updates)

let createService resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 2)

module Cosmos =

    open Equinox.Cosmos

    // in the happy path case, the event stream will typically be short, and the state cached, so snapshotting is less critical
    let accessStrategy = Equinox.Cosmos.AccessStrategy.Unoptimized
    // ... and there will generally be a single actor touching it at a given time, so we don't need to do a load (which would be more expensive than normal given the `accessStrategy`) before we sync
    let opt = Equinox.AllowStale
    let resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        fun id -> Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve(id, opt)
    let createService (context, cache) = createService (resolve (context, cache))