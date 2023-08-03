module ProjectorTemplate.Store

module Metrics =

    let log = Serilog.Log.ForContext("isMetric", true)

let private defaultCacheDuration = System.TimeSpan.FromMinutes 20
let private cacheStrategy cache = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)

// #if (cosmos || esdb || sss)
module Cosmos =

    let private createCached name codec initial fold accessStrategy (context, cache): Equinox.Category<_, _, _> =
        Equinox.CosmosStore.CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context, cache)

// #endif
module Dynamo =

    let private createCached name codec initial fold accessStrategy (context, cache): Equinox.Category<_, _, _> =
        Equinox.DynamoStore.DynamoStoreCategory(context, name, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context, cache)

module Esdb =

    let create name codec initial fold (context, cache) =
        let accessStrategy = Equinox.EventStoreDb.AccessStrategy.Unoptimized
        Equinox.EventStoreDb.EventStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

module Sss =

    let create name codec initial fold (context, cache) =
        let accessStrategy = Equinox.SqlStreamStore.AccessStrategy.Unoptimized
        Equinox.SqlStreamStore.SqlStreamStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

#if esdb
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Context =
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Cache
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Cache
#endif
