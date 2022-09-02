module Shipping.Domain.FinalizationTransaction

let [<Literal>] Category = "FinalizationTransaction"
let streamName (transactionId : TransactionId) = struct (Category, TransactionId.toString transactionId)
let (|StreamName|_|) = function FsCodec.StreamName.CategoryAndId (Category, TransactionId.Parse transId) -> Some transId | _ -> None

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | FinalizationRequested of {| container : ContainerId; shipments : ShipmentId array |}
        | ReservationCompleted
        /// Signifies we're switching focus to relinquishing any assignments we completed.
        /// The list includes any items we could possibly have touched (including via idempotent retries)
        | RevertCommenced       of {| shipments : ShipmentId array |}
        | AssignmentCompleted
        /// Signifies all processing for the transaction has completed - the Watchdog looks for this event
        | Completed
        | Snapshotted           of {| state : State |}
        interface TypeShape.UnionContract.IUnionContract

    and // covered by autoUnionToJsonObject: [<System.Text.Json.Serialization.JsonConverter(typeof<FsCodec.SystemTextJson.UnionConverter<State>>)>]
        State =
        | Initial
        | Reserving of {| container : ContainerId; shipments : ShipmentId array |}
        | Reverting of {| shipments : ShipmentId array |}
        | Assigning of {| container : ContainerId; shipments : ShipmentId array |}
        | Assigned  of {| container : ContainerId; shipments : ShipmentId array |}
        | Completed of {| success : bool |}
    let codec, codecJe = Config.EventCodec.gen<Event>, Config.EventCodec.genJsonElement<Event>

module Reactions =

    /// Used by the Watchdog to infer whether a given event signifies that the processing has reached a terminal state
    let isTerminalEvent (encoded : FsCodec.ITimelineEvent<_>) =
        encoded.EventType = nameof(Events.Completed)

module Fold =

    type State = Events.State
    let initial : State = State.Initial

    // The implementation trusts (does not spend time double checking) that events have passed an isValidTransition check
    let evolve (state : State) (event : Events.Event) : State =
        match state, event with
        | _,                    Events.FinalizationRequested e -> State.Reserving {| container = e.container; shipments = e.shipments |}
        | State.Reserving s,    Events.ReservationCompleted -> State.Assigning {| container = s.container; shipments = s.shipments |}
        | State.Reserving _s,   Events.RevertCommenced e    -> State.Reverting {| shipments = e.shipments |}
        | State.Reverting _s,   Events.Completed            -> State.Completed {| success = false |}
        | State.Assigning s,    Events.AssignmentCompleted  -> State.Assigned  {| container = s.container; shipments = s.shipments |}
        | State.Assigned _,     Events.Completed            -> State.Completed {| success = true |}
        | _,                    Events.Snapshotted state    -> state.state
        // this shouldn't happen, but, if we did produce invalid events, we'll just ignore them
        | state, _                                          -> state
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot state = Events.Snapshotted {| state = state |}

module Flow =

    type Action =
        | ReserveShipments      of shipmentIds : ShipmentId array
        | RevertReservations    of shipmentIds : ShipmentId array
        | AssignShipments       of shipmentIds : ShipmentId array * containerId : ContainerId
        | FinalizeContainer     of containerId : ContainerId * shipmentIds : ShipmentId array
        | Finish                of success : bool

    let nextAction : Fold.State -> Action = function
        | Fold.State.Reserving s -> Action.ReserveShipments   s.shipments
        | Fold.State.Reverting s -> Action.RevertReservations s.shipments
        | Fold.State.Assigning s -> Action.AssignShipments   (s.shipments, s.container)
        | Fold.State.Assigned s ->  Action.FinalizeContainer (s.container, s.shipments)
        | Fold.State.Completed r -> Action.Finish             r.success
        // As all state transitions are driven by FinalizationProcess, we can rule this out
        | Fold.State.Initial as s      -> failwith (sprintf "Cannot interpret state %A" s)

    let isValidTransition (event : Events.Event) (state : Fold.State) =
        match state, event with
        | Fold.State.Initial,       Events.FinalizationRequested _
        | Fold.State.Reserving _,   Events.RevertCommenced _
        | Fold.State.Reserving _,   Events.ReservationCompleted _
        | Fold.State.Reverting _,   Events.Completed
        | Fold.State.Assigning _,   Events.AssignmentCompleted _
        | Fold.State.Assigned _,    Events.Completed -> true
        | _ -> false

    // If there are no events to apply to the state, it pushes the transaction manager to
    // follow up on the next action from where it was.
    let decide (update : Events.Event option) (state : Fold.State) : Action * Events.Event list =
        let events =
            match update with
            | Some e when isValidTransition e state -> [e]
            | _ -> []

        let state' = Fold.fold state events
        nextAction state', events

type Service internal (resolve : TransactionId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Step(transactionId, maybeUpdate) : Async<Flow.Action> =
        let decider = resolve transactionId
        decider.Transact(Flow.decide maybeUpdate)

module Config =

    let private resolve = function
        | Config.Store.Memory store ->
            Config.Memory.create Events.codec Fold.initial Fold.fold store
            |> Equinox.Decider.resolve Config.log
        | Config.Store.Cosmos (context, cache) ->
            Config.Cosmos.createSnapshotted Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
            |> Equinox.Decider.resolve Config.log
        | Config.Store.Dynamo (context, cache) ->
            Config.Dynamo.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
            |> Equinox.Decider.resolve Config.log
    let create store = Service(streamName >> resolve store)
