module Shipping.Domain.Container

let [<Literal>] private Category = "Container"
let streamName (containerId : ContainerId) = FsCodec.StreamName.create Category (ContainerId.toString containerId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Finalized   of {| shipmentIds : ShipmentId[] |}
        | Snapshotted of {| shipmentIds : ShipmentId[] |}
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

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

type Service internal (resolve : ContainerId -> Equinox.Stream<Events.Event, Fold.State>) =

    member _.Finalize(containerId, shipmentIds) : Async<unit> =
        let decider = resolve containerId
        decider.Transact(interpretFinalize shipmentIds)

let create resolveStream =
    let resolve id = Equinox.Stream(Serilog.Log.ForContext<Service>(), resolveStream (streamName id), maxAttempts=3)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
