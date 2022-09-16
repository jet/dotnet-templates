module ProjectorTemplate.Config

let log = Serilog.Log.ForContext("isMetric", true)

// #if (esdb || sss || cosmos)
module Cosmos =

    let private createCached codec initial fold accessStrategy (context, cache) : Equinox.Category<_, _, _> =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

    let createRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCached codec initial fold accessStrategy (context, cache)

// #endif
module Dynamo =

    let private createCached codec initial fold accessStrategy (context, cache) : Equinox.Category<_, _, _> =
        let cacheStrategy = Equinox.DynamoStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.DynamoStore.DynamoStoreCategory(context, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

    let createRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.RollingState toSnapshot
        createCached codec initial fold accessStrategy (context, cache)

#if !(sourceKafka && kafka)
module Esdb =

    let create codec initial fold (context, cache) =
        let cacheStrategy = Equinox.EventStoreDb.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.EventStoreDb.EventStoreCategory(context, codec, fold, initial, cacheStrategy)

module Sss =

    let create codec initial fold (context, cache) =
        let cacheStrategy = Equinox.SqlStreamStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.SqlStreamStore.SqlStreamStoreCategory(context, codec, fold, initial, cacheStrategy)

#endif

// #if (esdb || sss || dynamo)    
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Store =
#if (esdb || sss)
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
#endif    
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
// #endif
