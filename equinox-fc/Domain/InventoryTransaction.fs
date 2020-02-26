/// Process Manager used to:
/// - Coordinate competing attempts to transfer quantities from stock; if balance is 3 one of contesting requests to remove 2 or 3 items must reach `Failed`
/// - maintain rolling balance of stock levels per Location
/// - while recording any transfers or adjustment in an overall Inventory record
/// The Process is driven by two collaborating actors:
/// 1) The 'happy path', where a given actor is executing the known steps of the command flow
///    In the normal case, such an actor will bring the flow to a terminal state (Completed or Failed)
/// 2) A watchdog-projector, which reacts to observed events in this Category by stepping in to complete in-flight requests that have stalled
///    This represents the case where a 'happy path' actor died, or experienced another impediment on the path.
module Fc.Inventory.Transaction

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
        | Logged
        | Completed
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

type Action =
    | Adjust of LocationId * int
    | Remove of LocationId * int
    | Add of LocationId * int
    | Log of LoggingState
    | Finish of success : bool
and LoggingState =
    | Adjusted of Events.AdjustmentRequested
    | Transferred of Added
and Added = { request : Events.TransferRequested; removed : Events.Removed; added : Events.Added }

module Fold =

    type State =
        | Initial
        | Running of RunningState
        | Logging of LoggingState
        | Completed of TerminalState
    and RunningState =
        | Adjust of Events.AdjustmentRequested
        | Transfer of TransferState
    and TransferState =
        | Requested of Events.TransferRequested
        | Adding of Removed
    and TerminalState =
        | Adjusted of Events.AdjustmentRequested
        | Transferred of Added
        | TransferFailed of Events.TransferRequested
    and Removed = { request : Events.TransferRequested; removed : Events.Removed }
    let initial = Initial
    let evolve state event =
        match state, event with
        (* Adjustment Process *)
        | Initial, Events.AdjustmentRequested r ->
            Running (Adjust r)
        | Running (Adjust r), Events.Adjusted ->
            Logging (LoggingState.Adjusted r)

        (* Transfer Process *)
        | Initial, Events.TransferRequested e ->
            Running (Transfer (Requested e))

        | Running (Transfer (Requested s)), Events.Failed ->
            Completed (TransferFailed s)

        | Running (Transfer (Requested s)), Events.Removed e ->
            Running (Transfer (Adding { request = s; removed = e }))
        | Running (Transfer (Adding s)), Events.Added e ->
            Logging (LoggingState.Transferred { request = s.request; removed = s.removed; added = e  })

        (* Log result *)
        | Logging (LoggingState.Adjusted s), Events.Logged ->
            Completed (Adjusted s)
        | Logging (LoggingState.Transferred s), Events.Logged ->
            Completed (Transferred s)

        (* Any disallowed state changes represent gaps in the model, so we fail fast *)
        | state, event -> failwithf "Unexpected %A when %A" event state
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    /// Validates an event actually represents an acceptable, non-redundant state transition
    let filter event state =
        match state, event with
        | Initial,                          Events.AdjustmentRequested _
        | Initial,                          Events.TransferRequested _
        | Running (Adjust _),               Events.Adjusted
        | Running (Transfer (Requested _)), Events.Failed
        | Running (Transfer (Requested _)), Events.Removed _
        | Running (Transfer (Adding _)),    Events.Added _
        | Logging _,                        Events.Logged ->
            [event]
        | _ -> []

    /// Determines the next action (if any) to be carried out in this workflwo
    let nextAction : State -> Action = function
        | Initial -> failwith "Cannot interpret Initial state"
        | Running (Adjust r) -> Action.Adjust (r.location, r.quantity)
        | Running (Transfer (Requested r)) -> Action.Remove (r.source, r.quantity)
        | Running (Transfer (Adding r)) -> Action.Add (r.request.destination, r.request.quantity)
        | Logging s -> Action.Log s
        | Completed (TransferFailed _) -> Finish false
        | Completed (Transferred _ | Adjusted _) -> Finish true

/// Given an event from the Process's timeline, yields the State, in order that it can be completed
let decide update (state : Fold.State) : Action * Events.Event list =
    let events =
        match update with
        | None -> []
        | Some update -> Fold.filter update state
    let state' = Fold.fold state events
    Fold.nextAction state', events

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For streamId) = Equinox.Stream<Events.Event, Fold.State>(log, resolve streamId, maxAttempts)

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)

let createService resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)

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

/// Handles requirement to infer when a transaction is 'stuck'
/// Note we don't want to couple to the state in a deep manner; thus we track:
/// a) when the request intent is established (aka when *Requested is logged)
/// b) when a transaction reports that it has Completed
module Watchdog =

    module Events =

        // TOCONSIDER: this can be written more generically by just grabbing the time of the very first event
        type Event =
            | AdjustmentRequested
            | TransferRequested
            | Completed
            interface TypeShape.UnionContract.IUnionContract
        type TimestampAndEvent = System.DateTimeOffset * Event
        let codec =
            let up (encoded : FsCodec.ITimelineEvent<_>, message) : TimestampAndEvent = encoded.Timestamp, message
            let down _union = failwith "Not Implemented"
            FsCodec.NewtonsoftJson.Codec.Create<TimestampAndEvent, Event, (*'Meta*)obj>(up, down)

    module Fold =

        type State = Initial | Active of startTime: System.DateTimeOffset | Completed
        let initial = Initial
        let evolve _state = function
            | startTime, (Events.AdjustmentRequested | Events.TransferRequested ) -> Active startTime
            | _, Events.Completed -> Completed
        let fold : State -> Events.TimestampAndEvent seq -> State = Seq.fold evolve

    type Status = Complete | Active | Stuck
    let categorize cutoffTime = function
        | Fold.Initial -> Active // cover (hypothetical) corner case where we don't have any valid events yet
        | Fold.Active startTime when startTime < cutoffTime -> Stuck
        | Fold.Active _ -> Active
        | Fold.Completed -> Complete
