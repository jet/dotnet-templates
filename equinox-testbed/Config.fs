module TestbedTemplate.Config

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider cat = Equinox.Decider.resolve log cat

module EventCodec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create()
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions)
    let genJe<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>(options = defaultOptions)

module Memory =

    let create _codec initial fold store : Equinox.Category<_, _, _> =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial)

module Cosmos =

    let create codec initial fold cacheStrategy accessStrategy context =
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

module Esdb =

    let create codec initial fold cacheStrategy accessStrategy context =
        Equinox.EventStoreDb.EventStoreCategory(context, codec, fold, initial, ?caching = cacheStrategy, ?access = accessStrategy)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Store =
//#if (memoryStore || (!cosmos && !eventStore))
    | Memory of Equinox.MemoryStore.VolatileStore<obj>
//#endif
//#if cosmos
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.CosmosStore.CachingStrategy * unfolds: bool
//#endif
//#if eventStore
    | Esdb of Equinox.EventStoreDb.EventStoreContext * Equinox.EventStoreDb.CachingStrategy option * unfolds: bool
//#endif
