module Shipping.Domain.FinalizationTransaction

let [<Literal>] private CategoryName = "FinalizationTransaction"
let private streamId = FsCodec.StreamId.gen TransactionId.toString
let private catId = CategoryId(CategoryName, streamId, FsCodec.StreamId.dec TransactionId.parse)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | FinalizationRequested of  {| container: ContainerId; shipments: ShipmentId[] |}
        | ReservationCompleted
        /// Signifies we're switching focus to relinquishing any assignments we completed.
        /// The list includes any items we could possibly have touched (including via idempotent retries)
        | RevertCommenced of        {| shipments: ShipmentId[] |}
        | AssignmentCompleted
        /// Signifies all processing for the transaction has completed - the Watchdog looks for this event
        | Completed
        | [<DataMember(Name = "Snapshotted")>] Snapshotted of {| state: State |}
        interface TypeShape.UnionContract.IUnionContract

    and // covered by autoUnionToJsonObject: [<System.Text.Json.Serialization.JsonConverter(typeof<FsCodec.SystemTextJson.UnionConverter<State>>)>]
        State =
        | Initial
        | Reserving of              {| container: ContainerId; shipments: ShipmentId[] |}
        | Reverting of              {| shipments: ShipmentId[] |}
        | Assigning of              {| container: ContainerId; shipments: ShipmentId[] |}
        | Assigned  of              {| container: ContainerId; shipments: ShipmentId[] |}
        | Completed of              {| success: bool |}
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Reactions =

    /// Used by the Watchdog to infer whether a given event signifies that the processing has reached a terminal state
    let isTerminalEvent (encoded: FsCodec.ITimelineEvent<_>) =
        encoded.EventType = nameof Events.Completed
    let [<Literal>] categoryName = CategoryName
    let [<return: Struct>] (|For|_|) = catId.TryDecode

module Fold =

    type State = Events.State
    let initial: State = State.Initial

    module Snapshot =

        let generate state = Events.Snapshotted {| state = state |}
        let isOrigin = function Events.Snapshotted _ -> true | _ -> false
        let config = isOrigin, generate

    let evolve (s: State) = function
        | Events.Snapshotted s ->           s.state
        | Events.FinalizationRequested e -> State.Reserving {| container = e.container; shipments = e.shipments |}
        | Events.ReservationCompleted ->    match s with
                                            | State.Reserving s -> State.Assigning {| container = s.container; shipments = s.shipments |}
                                            | s -> s
        | Events.RevertCommenced e ->       State.Reverting {| shipments = e.shipments |}
        | Events.AssignmentCompleted ->     match s with
                                            | State.Assigning s -> State.Assigned  {| container = s.container; shipments = s.shipments |}
                                            | s -> s
        | Events.Completed ->               match s with
                                            | State.Reverting _ ->  State.Completed {| success = false |}
                                            | State.Assigned _ ->   State.Completed {| success = true |}
                                            | s -> s
    let fold: State -> Events.Event[] -> State = Array.fold evolve

/// Manages the Workflow aspect, mapping Fold.State to an Action surfacing information relevant to reactions processing
module Flow =

    type State = Fold.State
    type Phase =
        | ReserveShipments of       shipmentIds: ShipmentId[]
        | RevertReservations of     shipmentIds: ShipmentId[]
        | AssignShipments of        shipmentIds: ShipmentId[] * containerId: ContainerId
        | FinalizeContainer of      containerId: ContainerId * shipmentIds: ShipmentId[]
        | Finish of                 success: bool

    let phase: Fold.State -> Phase = function
        | State.Reserving s ->      Phase.ReserveShipments   s.shipments
        | State.Reverting s ->      Phase.RevertReservations s.shipments
        | State.Assigning s ->      Phase.AssignShipments   (s.shipments, s.container)
        | State.Assigned s ->       Phase.FinalizeContainer (s.container, s.shipments)
        | State.Completed r ->      Phase.Finish             r.success
        // As all state transitions are driven by FinalizationProcess, we can rule this out
        | State.Initial as s ->     failwith $"Cannot interpret state %A{s}"
    let tryStart containerId shipmentIds = function
        | State.Initial ->           [| Events.FinalizationRequested {| container = containerId; shipments = shipmentIds |} |]
        | _ -> [||]
    let decideStartRevert failedReservations = function
        | State.Reserving s ->      [| Events.RevertCommenced {| shipments = s.shipments |> Array.except failedReservations |} |]
        | _ -> [||]
    let recordReservationCompleted = function
        | State.Reserving _ ->      [| Events.ReservationCompleted |]
        | _ -> [||]
    let recordAssignmentCompleted = function
        | State.Assigning _ ->      [| Events.AssignmentCompleted |]
        | _ -> [||]
    let recordCompleted = function
        | State.Reverting _
        | State.Assigned _ ->       [| Events.Completed |]
        | _ -> [||]
 
type Service internal (resolve: TransactionId -> Equinox.Decider<Events.Event, Fold.State>) =

    let exec transactionId (f: Fold.State -> Events.Event[]): Async<Flow.Phase> =
        let decider = resolve transactionId
        decider.Transact(f, render = Flow.phase)
    member _.Read transactionId: Async<Flow.Phase> =
        let decider = resolve transactionId
        decider.Query Flow.phase
    member _.Start(transactionId, containerId, shipments) = Flow.tryStart containerId shipments |> exec transactionId
    member _.StartRevert(transactionId, failedReservations) = Flow.decideStartRevert failedReservations |> exec transactionId
    member _.RecordReservationCompleted transactionId = Flow.recordReservationCompleted |> exec transactionId
    member _.RecordAssignmentCompleted transactionId = Flow.recordAssignmentCompleted |> exec transactionId
    member _.RecordCompleted transactionId = exec transactionId Flow.recordCompleted

module Factory =

    let private (|Category|) = function
        | Store.Config.Memory store ->            Store.Memory.create CategoryName Events.codec Fold.initial Fold.fold store
        | Store.Config.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted CategoryName Events.codecJe Fold.initial Fold.fold Fold.Snapshot.config (context, cache)
        | Store.Config.Dynamo (context, cache) -> Store.Dynamo.createSnapshotted CategoryName Events.codec Fold.initial Fold.fold Fold.Snapshot.config (context, cache)
        | Store.Config.Esdb (context, cache) ->   Store.Esdb.createUnoptimized CategoryName Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Service(streamId >> Store.createDecider cat)
