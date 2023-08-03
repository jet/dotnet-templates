module ReactorTemplate.Store

module Metrics =
    let log = Serilog.Log.ForContext("isMetric", true)
    
let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    open FsCodec.SystemTextJson

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>() // options = Options.Default
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>() // options = Options.Default

module Cosmos =

    let private createCached name codec initial fold accessStrategy (context, cache): Equinox.Category<_, _, _> =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context, cache)

module Dynamo =

    let private createCached name codec initial fold accessStrategy (context, cache): Equinox.Category<_, _, _> =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.DynamoStore.DynamoStoreCategory(context, name, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, accessStrategy, cacheStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.DynamoStore.AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context, cache)

#if !(sourceKafka && kafka)
module Esdb =

    let create name codec initial fold (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = Equinox.EventStoreDb.AccessStrategy.Unoptimized
        Equinox.EventStoreDb.EventStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

module Sss =

    let create name codec initial fold (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = Equinox.SqlStreamStore.AccessStrategy.Unoptimized
        Equinox.SqlStreamStore.SqlStreamStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

#endif
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Context =
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Cache
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Cache
#if !(sourceKafka && kafka)
    | Esdb of Equinox.EventStoreDb.EventStoreContext * Equinox.Cache
    | Sss of Equinox.SqlStreamStore.SqlStreamStoreContext * Equinox.Cache
#endif
