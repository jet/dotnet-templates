module Shipping.Domain.Container

let [<Literal>] Category = "Container"
let streamId = Equinox.StreamId.gen ContainerId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Finalized   of {| shipmentIds : ShipmentId array |}
        | Snapshotted of {| shipmentIds : ShipmentId array |}
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Config.EventCodec.gen<Event>, Config.EventCodec.genJsonElement<Event>

module Fold =

    type State = { shipmentIds : ShipmentId array }
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

    let private (|Category|) = function
        | Config.Store.Memory store ->            Config.Memory.create Events.codec Fold.initial Fold.fold store
        | Config.Store.Cosmos (context, cache) -> Config.Cosmos.createSnapshotted Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Config.Store.Dynamo (context, cache) -> Config.Dynamo.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Config.Store.Esdb (context, cache) ->   Config.Esdb.createUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Service(streamId >> Config.createDecider cat Category)
