module Shipping.Domain.Container

let [<Literal>] private Category = "Container"
let streamName (containerId : ContainerId) = FsCodec.StreamName.create Category (ContainerId.toString containerId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Finalized   of {| shipmentIds : ShipmentId[] |}
        | Snapshotted of {| shipmentIds : ShipmentId[] |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Config.EventCodec.create<Event>()

module Fold =

    type State = { shipmentIds : ShipmentId[] }
    let initial = { shipmentIds = Array.empty }

    let evolve (_state : State) (event : Events.Event) : State =
        match event with
        | Events.Snapshotted snapshot -> { shipmentIds = snapshot.shipmentIds }
        | Events.Finalized event -> { shipmentIds = event.shipmentIds }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (state : State) = Events.Snapshotted {| shipmentIds = state.shipmentIds |}

let interpretFinalize shipmentIds (state : Fold.State): Events.Event list =
    [ if Array.isEmpty state.shipmentIds then yield Events.Finalized {| shipmentIds = shipmentIds |} ]

type Service internal (resolve : ContainerId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Finalize(containerId, shipmentIds) : Async<unit> =
        let decider = resolve containerId
        decider.Transact(interpretFinalize shipmentIds)

module Config =

    let private resolveStream = function
        | Config.Store.Memory store ->
            let cat = Config.Memory.create Events.codec Fold.initial Fold.fold store
            cat.Resolve
        | Config.Store.Cosmos (context, cache) ->
            let cat = Config.Cosmos.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
            cat.Resolve
    let private resolveDecider store = streamName >> resolveStream store >> Config.createDecider
    let create = resolveDecider >> Service
