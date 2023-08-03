module Shipping.Domain.FinalizationTransaction

module Stream =
    let [<Literal>] Category = "FinalizationTransaction"
    let id = Equinox.StreamId.gen TransactionId.toString
    let [<return: Struct>] (|Match|_|) = function FsCodec.StreamName.CategoryAndId (Category, TransactionId.Parse transId) -> ValueSome transId | _ -> ValueNone

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | FinalizationRequested of {| container: ContainerId; shipments: ShipmentId[] |}
        | ReservationCompleted
        /// Signifies we're switching focus to relinquishing any assignments we completed.
        /// The list includes any items we could possibly have touched (including via idempotent retries)
        | RevertCommenced       of {| shipments: ShipmentId[] |}
        | AssignmentCompleted
        /// Signifies all processing for the transaction has completed - the Watchdog looks for this event
        | Completed
        | Snapshotted           of {| state: State |}
        interface TypeShape.UnionContract.IUnionContract

    and // covered by autoUnionToJsonObject: [<System.Text.Json.Serialization.JsonConverter(typeof<FsCodec.SystemTextJson.UnionConverter<State>>)>]
        State =
        | Initial
        | Reserving of {| container: ContainerId; shipments: ShipmentId[] |}
        | Reverting of {| shipments: ShipmentId[] |}
        | Assigning of {| container: ContainerId; shipments: ShipmentId[] |}
        | Assigned  of {| container: ContainerId; shipments: ShipmentId[] |}
        | Completed of {| success: bool |}
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Reactions =

    /// Used by the Watchdog to infer whether a given event signifies that the processing has reached a terminal state
    let isTerminalEvent (encoded: FsCodec.ITimelineEvent<_>) =
        encoded.EventType = nameof(Events.Completed)

module Fold =

    type State = Events.State
    let initial: State = State.Initial

    // The implementation trusts (does not spend time double checking) that events have passed an isValidTransition check
    let evolve (state: State) (event: Events.Event): State =
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
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot state = Events.Snapshotted {| state = state |}

/// Manages the Workflow aspect, mapping Fold.State to an Action surfacing information relevant to reactions processing
module Flow =

    type Action =
        | ReserveShipments      of shipmentIds: ShipmentId[]
        | RevertReservations    of shipmentIds: ShipmentId[]
        | AssignShipments       of shipmentIds: ShipmentId[] * containerId: ContainerId
        | FinalizeContainer     of containerId: ContainerId * shipmentIds: ShipmentId[]
        | Finish                of success: bool

    let nextAction: Fold.State -> Action = function
        | Fold.State.Reserving s -> Action.ReserveShipments   s.shipments
        | Fold.State.Reverting s -> Action.RevertReservations s.shipments
        | Fold.State.Assigning s -> Action.AssignShipments   (s.shipments, s.container)
        | Fold.State.Assigned s ->  Action.FinalizeContainer (s.container, s.shipments)
        | Fold.State.Completed r -> Action.Finish             r.success
        // As all state transitions are driven by FinalizationProcess, we can rule this out
        | Fold.State.Initial as s      -> failwith $"Cannot interpret state %A{s}"

    let isValidTransition (event: Events.Event) (state: Fold.State) =
        match state, event with
        | Fold.State.Initial,       Events.FinalizationRequested _
        | Fold.State.Reserving _,   Events.RevertCommenced _
        | Fold.State.Reserving _,   Events.ReservationCompleted _
        | Fold.State.Reverting _,   Events.Completed
        | Fold.State.Assigning _,   Events.AssignmentCompleted _
        | Fold.State.Assigned _,    Events.Completed -> true
        | _ -> false

    let decide (update: Events.Event option) (state: Fold.State) = [|
        match update with
        | Some e when isValidTransition e state -> e
        | _ -> () |]

type Service internal (resolve: TransactionId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// (Optionally) idempotently applies an event representing progress achieved in some aspect of the workflow
    /// Yields a `Flow.Action` representing the next activity to be performed as implied by the workflow's State afterwards
    /// The workflow concludes when the action returned is `Action.Completed`
    member _.Step(transactionId, maybeUpdate): Async<Flow.Action> =
        let decider = resolve transactionId
        decider.Transact(Flow.decide maybeUpdate, Flow.nextAction)

module Factory =

    let private (|Category|) = function
        | Store.Context.Memory store ->            Store.Memory.create Stream.Category Events.codec Fold.initial Fold.fold store
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Stream.Category Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Context.Dynamo (context, cache) -> Store.Dynamo.createSnapshotted Stream.Category Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Context.Esdb (context, cache) ->   Store.Esdb.createUnoptimized Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Service(Stream.id >> Store.createDecider cat)
