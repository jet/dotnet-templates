module Shipping.Domain.FinalizationTransaction

let [<Literal>] private Category = "FinalizationTransaction"
let streamName (transactionId : TransactionId) = FsCodec.StreamName.create Category (TransactionId.toString transactionId)
let (|StreamName|_|) = function FsCodec.StreamName.CategoryAndId (Category, TransactionId.Parse transId) -> Some transId | _ -> None

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | FinalizationRequested of {| container : ContainerId; shipments : ShipmentId[] |}
        | ReservationCompleted
        /// Signifies we're switching focus to relinquishing any assignments we completed.
        /// The list includes any items we could possibly have touched (including via idempotent retries)
        | RevertCommenced       of {| shipments : ShipmentId[] |}
        | AssignmentCompleted
        /// Signifies all processing for the transaction has completed - the Watchdog looks for this event
        | Completed
        | Snapshotted           of {| state : State |}
        interface TypeShape.UnionContract.IUnionContract

    and [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.UnionConverter>)>]
        State =
        | Initial
        | Reserving of {| container : ContainerId; shipments : ShipmentId[] |}
        | Reverting of {| shipments : ShipmentId[] |}
        | Assigning of {| container : ContainerId; shipments : ShipmentId[] |}
        | Assigned  of {| container : ContainerId; shipments : ShipmentId[] |}
        | Completed of {| success : bool |}

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

/// Used by the Watchdog to infer whether a given event signifies that the processing has reached a terminal state
module Reactions =

    let isTerminalEvent (encoded : FsCodec.ITimelineEvent<_>) =
        encoded.EventType = "Completed" // TODO nameof(Completed)

module Fold =

    type State = Events.State
    let initial : State = State.Initial

    let isValidTransition (event : Events.Event) (state : State) =
        match state, event with
        | State.Initial,     Events.FinalizationRequested _
        | State.Reserving _, Events.RevertCommenced _
        | State.Reserving _, Events.ReservationCompleted _
        | State.Reverting _, Events.Completed
        | State.Assigning _, Events.AssignmentCompleted _
        | State.Assigned _,  Events.Completed -> true
        | _ -> false

    // The implementation trusts (does not spend time double checking) that events have passed an isValidTransition check
    let evolve (state : State) (event : Events.Event) : State =
        match state, event with
        | _, Events.FinalizationRequested event                -> State.Reserving {| container = event.container; shipments = event.shipments |}
        | State.Reserving state,  Events.ReservationCompleted  -> State.Assigning {| container = state.container; shipments = state.shipments |}
        | State.Reserving _state, Events.RevertCommenced event -> State.Reverting {| shipments = event.shipments |}
        | State.Reverting _state, Events.Completed             -> State.Completed {| success = false |}
        | State.Assigning state,  Events.AssignmentCompleted   -> State.Assigned  {| container = state.container; shipments = state.shipments |}
        | State.Assigned _,       Events.Completed             -> State.Completed {| success = true |}
        | _,                      Events.Snapshotted state     -> state.state
        // this shouldn't happen, but, if we did produce invalid events, we'll just ignore them
        | state, _                                             -> state
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot state = Events.Snapshotted {| state = state |}

[<RequireQualifiedAccess>]
type Action =
    | ReserveShipments   of shipmentIds : ShipmentId[]
    | RevertReservations of shipmentIds : ShipmentId[]
    | AssignShipments    of shipmentIds : ShipmentId[] * containerId : ContainerId
    | FinalizeContainer  of containerId : ContainerId * shipmentIds : ShipmentId[]
    | Finish             of success : bool

let nextAction : Fold.State -> Action = function
    | Fold.State.Reserving state      -> Action.ReserveShipments   state.shipments
    | Fold.State.Reverting state      -> Action.RevertReservations state.shipments
    | Fold.State.Assigning state      -> Action.AssignShipments   (state.shipments, state.container)
    | Fold.State.Assigned state       -> Action.FinalizeContainer (state.container, state.shipments)
    | Fold.State.Completed result     -> Action.Finish             result.success
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

type Service internal (resolve : TransactionId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Record(transactionId, update) : Async<Action> =
        let decider = resolve transactionId
        decider.Transact(decide update)

module Config =

    let create resolveStream =
        let resolve = streamName >> resolveStream >> Equinox.createDecider
        Service(resolve)

    module Cosmos =

        open Equinox.CosmosStore

        let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
        let create (context, cache) =
            let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
            let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
            create cat.Resolve
