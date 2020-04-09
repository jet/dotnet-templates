module Shipping.Domain.Container

let [<Literal>] private Category = "Container"
let streamName (containerId : ContainerId) = FsCodec.StreamName.create Category (ContainerId.toString containerId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | ContainerFinalized
        | Snapshotted of {| finalized : bool |}
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = { finalized : bool }
    let initial = { finalized = false }

    let evolve (state : State) (event : Events.Event) : State =
        match event with
        | Events.Snapshotted snapshot -> { finalized = snapshot.finalized }
        | Events.ContainerFinalized _ -> { state with finalized = true }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (state : State) = Events.Snapshotted {| finalized = state.finalized |}

let interpretFinalize (state : Fold.State): Events.Event list =
    [ if not state.finalized then yield Events.ContainerFinalized ]

type Service internal (resolve : ContainerId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Finalize(containerId) : Async<unit> =
        let stream = resolve containerId
        stream.Transact(interpretFinalize)

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
