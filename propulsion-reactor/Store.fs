module ReactorTemplate.Store

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider cat = Equinox.Decider.resolve log cat

module Codec =

    open FsCodec.SystemTextJson

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>() // options = Options.Default
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>() // options = Options.Default

module Cosmos =

    let private createCached codec initial fold accessStrategy (context, cache): Equinox.Category<_, _, _> =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

    let createRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCached codec initial fold accessStrategy (context, cache)

module Dynamo =

    let private createCached codec initial fold accessStrategy (context, cache): Equinox.Category<_, _, _> =
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
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Context =
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
#if !(sourceKafka && kafka)
    | Esdb of Equinox.EventStoreDb.EventStoreContext * Equinox.Core.ICache
    | Sss of Equinox.SqlStreamStore.SqlStreamStoreContext * Equinox.Core.ICache
#endif
