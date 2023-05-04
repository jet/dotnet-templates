module Shipping.Domain.Container

let [<Literal>] Category = "Container"
let streamId = Equinox.StreamId.gen ContainerId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Finalized   of {| shipmentIds: ShipmentId[] |}
        | Snapshotted of {| shipmentIds: ShipmentId[] |}
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Store.EventCodec.gen<Event>, Store.EventCodec.genJsonElement<Event>

module Fold =

    type State = { shipmentIds: ShipmentId[] }
    let initial = { shipmentIds = Array.empty }

    let evolve (_state: State) (event: Events.Event): State =
        match event with
        | Events.Snapshotted snapshot -> { shipmentIds = snapshot.shipmentIds }
        | Events.Finalized event -> { shipmentIds = event.shipmentIds }
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (state: State) = Events.Snapshotted {| shipmentIds = state.shipmentIds |}

let interpretFinalize shipmentIds (state: Fold.State): Events.Event list =
    [ if Array.isEmpty state.shipmentIds then yield Events.Finalized {| shipmentIds = shipmentIds |} ]

type Service internal (resolve: ContainerId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Finalize(containerId, shipmentIds): Async<unit> =
        let decider = resolve containerId
        decider.Transact(interpretFinalize shipmentIds)

module Factory =

    let private (|Category|) = function
        | Store.Context.Memory store ->            Store.Memory.create Events.codec Fold.initial Fold.fold store
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Context.Dynamo (context, cache) -> Store.Dynamo.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Context.Esdb (context, cache) ->   Store.Esdb.createUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Service(streamId >> Store.createDecider cat Category)
