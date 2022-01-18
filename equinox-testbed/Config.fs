module TestbedTemplate.Config

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)

module EventCodec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create()
    let create<'t when 't :> TypeShape.UnionContract.IUnionContract> () =
        Codec.Create<'t>(options = defaultOptions).ToByteArrayCodec()

module Memory =

    let create _codec initial fold store =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial)

module Cosmos =

    let create codec initial fold cacheStrategy accessStrategy context =
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

module Esdb =

    let create codec initial fold cacheStrategy accessStrategy context =
        Equinox.EventStore.EventStoreCategory(context, codec, fold, initial, ?caching = cacheStrategy, ?access = accessStrategy)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Store =
//#if (memoryStore || (!cosmos && !eventStore))
    | Memory of Equinox.MemoryStore.VolatileStore<obj>
//#endif
//#if cosmos
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.CosmosStore.CachingStrategy * unfolds: bool
//#endif
//#if eventStore
    | Esdb of Equinox.EventStore.EventStoreContext * Equinox.EventStore.CachingStrategy option * unfolds: bool
//#endif
