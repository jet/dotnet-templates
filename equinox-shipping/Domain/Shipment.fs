module Shipping.Domain.Shipment

let [<Literal>] private CategoryName = "Shipment"
let private streamId = FsCodec.StreamId.gen ShipmentId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Reserved    of {| transaction: TransactionId |}
        | Assigned    of {| container: ContainerId |}
        | Revoked
        | Snapshotted of {| reservation: TransactionId option; association: ContainerId option |}
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Fold =

    type State = { reservation: TransactionId option; association: ContainerId option }
    let initial: State = { reservation = None; association = None }

    let evolve (state: State) = function
        | Events.Reserved event     -> { reservation = Some event.transaction; association = None }
        | Events.Revoked            ->   initial
        | Events.Assigned event     -> { state with                            association = Some event.container  }
        | Events.Snapshotted event  -> { reservation = event.reservation;      association = event.association }
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (state: State) = Events.Snapshotted {| reservation = state.reservation; association = state.association |}

module Decisions =
    
    let reserve transactionId: Fold.State -> bool * Events.Event[] = function
        | { reservation = Some r } when r = transactionId -> true, [||]
        | { reservation = None } -> true, [| Events.Reserved {| transaction = transactionId |} |]
        | _ -> false, [||]

    let revoke transactionId: Fold.State -> Events.Event[] = function
        | { reservation = Some r; association = None } when r = transactionId ->
            [| Events.Revoked |]
        | _ -> [||] // Ignore if a) already revoked/never reserved b) not reserved for this transactionId

    let assign transactionId containerId: Fold.State -> Events.Event[] = function
        | { reservation = Some r; association = None } when r = transactionId ->
            [| Events.Assigned {| container = containerId |} |]
        | _ -> [||] // Ignore if a) this transaction was not the one reserving it or b) it's already been assigned

type Service internal (resolve: ShipmentId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.TryReserve(shipmentId, transactionId): Async<bool> =
        let decider = resolve shipmentId
        decider.Transact(Decisions.reserve transactionId)

    member _.Revoke(shipmentId, transactionId): Async<unit> =
        let decider = resolve shipmentId
        decider.Transact(Decisions.revoke transactionId)

    member _.Assign(shipmentId, containerId, transactionId): Async<unit> =
        let decider = resolve shipmentId
        decider.Transact(Decisions.assign transactionId containerId)

module Factory =

    let private (|Category|) = function
        | Store.Config.Memory store ->            Store.Memory.create CategoryName Events.codec Fold.initial Fold.fold store
        | Store.Config.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted CategoryName Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Config.Dynamo (context, cache) -> Store.Dynamo.createSnapshotted CategoryName Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Config.Esdb (context, cache) ->   Store.Esdb.createUnoptimized CategoryName Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Service(streamId >> Store.createDecider cat)
