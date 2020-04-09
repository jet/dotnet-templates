module Shipping.Domain.FinalizationTransaction

open System.Runtime.Serialization

let [<Literal>] private Category = "FinalizationTransaction"
let streamName (transactionId : TransactionId) = FsCodec.StreamName.create Category (TransactionId.toString transactionId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] private CompletedEventName = "Completed"
    type Event =
        | FinalizationRequested of {| containerId : ContainerId; shipmentIds : ShipmentId[] |}
        | AssignmentCompleted
        /// Signifies we're switching focus to relinquishing any assignments we completed.
        /// The list includes any items we could possibly have touched (including via idempotent retries)
        | RevertCommenced       of {| shipmentIds : ShipmentId[] |}
        /// Signifies all processing for the transaction has completed - the Watchdog looks for this event
        | [<DataMember(Name = CompletedEventName)>]Completed
        | Snapshotted           of State
        interface TypeShape.UnionContract.IUnionContract

    and [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.UnionConverter>)>]
        State =
        | Initial
        | Assigning of TransactionState
        | Assigned  of containerId : ContainerId
        | Reverting of TransactionState
        | Completed of success : bool
    and TransactionState = { container : ContainerId; shipments : ShipmentId[] }

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    /// Used by the Watchdog to infer whether a given event signifies that the processing has reached a terminal state
    let isTerminalEvent (encoded : FsCodec.ITimelineEvent<_>) =
        encoded.EventType = CompletedEventName

module Fold =

    type State = Events.State
    let initial : State = State.Initial

    let isValidTransition (event : Events.Event) (state : State) =
        match state, event with
        | State.Initial,     Events.FinalizationRequested _
        | State.Assigning _, Events.AssignmentCompleted _
        | State.Assigning _, Events.RevertCommenced _
        | State.Assigned _,  Events.Completed
        | State.Reverting _, Events.Completed -> true
        | _ -> false

    // The implementation trusts (does not spend time double checking) that events have passed an isValidTransition check
    let evolve (state : State) (event : Events.Event) : State =
        match state, event with
        | _, Events.FinalizationRequested event                -> State.Assigning { container = event.containerId; shipments = event.shipmentIds }
        | State.Assigning state,  Events.AssignmentCompleted   -> State.Assigned state.container
        | State.Assigning state,  Events.RevertCommenced event -> State.Reverting { state with shipments = event.shipmentIds }
        | State.Assigned _,       Events.Completed             -> State.Completed true
        | State.Reverting _state, Events.Completed             -> State.Completed false
        | _,                      Events.Snapshotted state     -> state
        // this shouldn't happen, but, if we did produce invalid events, we'll just ignore them
        | state, _                                             -> state
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot state = Events.Snapshotted state

[<RequireQualifiedAccess>]
type Action =
    | AssignShipments   of containerId : ContainerId * shipmentIds : ShipmentId[]
    | FinalizeContainer of containerId : ContainerId
    | RevertAssignment  of containerId : ContainerId * shipmentIds : ShipmentId[]
    | Finish            of success : bool

let nextAction : Fold.State -> Action = function
    | Fold.State.Assigning state      -> Action.AssignShipments   (state.container, state.shipments)
    | Fold.State.Assigned containerId -> Action.FinalizeContainer containerId
    | Fold.State.Reverting state      -> Action.RevertAssignment  (state.container, state.shipments)
    | Fold.State.Completed result     -> Action.Finish result
    // As all state transitions are driven by members on the FinalizationProcessManager, we can rule this out
    | Fold.State.Initial as s         -> failwith (sprintf "Cannot interpret state %A" s)

// If there are no events to apply to the state, it pushes the transaction manager to
// follow up on the next action from where it was.
let decide (update : Events.Event option) (state : Fold.State) : Action * Events.Event list =
    let events =
        match update with
        | Some e when Fold.isValidTransition e state -> [e]
        | _ -> []

    let state' = Fold.fold state events
    nextAction state', events

type Service internal (resolve : TransactionId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)

let private create resolve =
    let resolve id = Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve (streamName id), maxAttempts = 3)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve

/// Handles requirement to infer when a transaction is 'stuck'
/// Note we don't want to couple to the state in a deep manner; thus we track:
/// a) when the request intent is established (aka when *Requested is logged)
/// b) when a transaction reports that it has Completed
module Watchdog =

    module Events =

        type Event =
            | NonTerminal
            | Terminal
        type TimestampAndEvent = System.DateTimeOffset * Event
        let codec =
            let tryDecode (encoded : FsCodec.ITimelineEvent<byte[]>) =
                Some (encoded.Timestamp, if Events.isTerminalEvent encoded then Terminal else NonTerminal)
            let encode _ = failwith "Not Implemented"
            let mapCausation _ = failwith "Not Implemented"
            FsCodec.Codec.Create<TimestampAndEvent, _, obj>(encode, tryDecode, mapCausation)

    module Fold =

        type State = Initial | Active of startTime: System.DateTimeOffset | Completed
        let initial = Initial
        let evolve state = function
            | startTime, Events.NonTerminal ->
                if state = Initial then Active startTime
                else state
            | _, Events.Terminal ->
                Completed
        let fold : State -> Events.TimestampAndEvent seq -> State = Seq.fold evolve

    type Status = Complete | Active | Stuck
    let categorize cutoffTime = function
        | Fold.Initial -> failwith "Expected at least one valid event"
        | Fold.Active startTime when startTime < cutoffTime -> Stuck
        | Fold.Active _ -> Active
        | Fold.Completed -> Complete

    let fold : Events.TimestampAndEvent seq -> Fold.State =
        Fold.fold Fold.initial

    let (|FoldToWatchdogState|) events : Fold.State =
        events
        |> Seq.choose Events.codec.TryDecode
        |> fold

    let (|MatchCategory|_|) = function
        | FsCodec.StreamName.CategoryAndId (Category, TransactionId.Parse transId) -> Some transId
        | _ -> None
    let (|MatchState|_|) = function
        | MatchCategory transId, FoldToWatchdogState state -> Some (transId, state)
        | _ -> None
