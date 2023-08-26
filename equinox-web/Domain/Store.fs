module TodoBackendTemplate.Store

let log = Serilog.Log.ForContext("isMetric", true)
let resolveDecider cat = Equinox.Decider.forStream log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>() // options = Options.Default
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() // options = Options.Default

#if (memoryStore || (!cosmos && !dynamo && !eventStore))
module Memory =

    let create name _codec initial fold store: Equinox.Category<_, _, _> =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, name, FsCodec.Box.Codec.Create(), fold, initial)

#endif
//#if cosmos
module Cosmos =

    let private createCached name codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context, cache)

//#endif
//#if dynamo
module Dynamo =

    let private createCached name codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.DynamoStore.DynamoStoreCategory(context, name, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, accessStrategy, cacheStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context, cache)

//#endif
//#if eventStore
module Esdb =

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = Equinox.EventStoreDb.AccessStrategy.RollingSnapshots (isOrigin, toSnapshot)
        Equinox.EventStoreDb.EventStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

//#endif
[<NoComparison; NoEquality; RequireQualifiedAccess>]
#if (memoryStore || (!cosmos && !dynamo && !eventStore))
type Config<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
#else
type Config =
#endif
//#if cosmos
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Cache
//#endif
//#if dynamo
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Cache
//#endif
//#if eventStore
    | Esdb of Equinox.EventStoreDb.EventStoreContext * Equinox.Cache
//#endif
