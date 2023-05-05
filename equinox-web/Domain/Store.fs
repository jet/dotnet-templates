module TodoBackendTemplate.Store

let log = Serilog.Log.ForContext("isMetric", true)
let resolveDecider cat = Equinox.Decider.resolve log cat

module Codec =

    open FsCodec.SystemTextJson

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>() // options = Options.Default
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>() // options = Options.Default

#if (memoryStore || (!cosmos && !dynamo && !eventStore))
module Memory =

    let create _codec initial fold store : Equinox.Category<_, _, _> =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial)

#endif
//#if cosmos
module Cosmos =

    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

    let createRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCached codec initial fold accessStrategy (context, cache)

//#endif
//#if dynamo
module Dynamo =

    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.DynamoStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.DynamoStore.DynamoStoreCategory(context, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

    let createRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.RollingState toSnapshot
        createCached codec initial fold accessStrategy (context, cache)

//#endif
//#if eventStore
module Esdb =

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let cacheStrategy = Equinox.EventStoreDb.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = Equinox.EventStoreDb.AccessStrategy.RollingSnapshots (isOrigin, toSnapshot)
        Equinox.EventStoreDb.EventStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

//#endif
[<NoComparison; NoEquality; RequireQualifiedAccess>]
#if (memoryStore || (!cosmos && !dynamo && !eventStore))
type Context<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
#else
type Context =
#endif
//#if cosmos
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
//#endif
//#if dynamo
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
//#endif
//#if eventStore
    | Esdb of Equinox.EventStoreDb.EventStoreContext * Equinox.Core.ICache
//#endif
