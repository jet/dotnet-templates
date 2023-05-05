module Shipping.Domain.Store

/// Tag log entries so we can filter them out if logging to the console
let log = Serilog.Log.ForContext("isMetric", true)
let createDecider cat = Equinox.Decider.resolve log cat

module Codec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create(autoTypeSafeEnumToJsonString = true, autoUnionToJsonObject = true)
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions)
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>(options = defaultOptions)

module Memory =

    let create codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Deflate.EncodeUncompressed codec, fold, initial)

let defaultCacheDuration = System.TimeSpan.FromMinutes 20.

module Cosmos =

    open Equinox.CosmosStore
    
    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

module Dynamo =

    open Equinox.DynamoStore
    
    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, FsCodec.Deflate.EncodeTryDeflate codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

module Esdb =

    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.EventStoreDb.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        Equinox.EventStoreDb.EventStoreCategory(context, codec, fold, initial, cacheStrategy, ?access = accessStrategy)
    let createUnoptimized codec initial fold (context, cache) =
        createCached codec initial fold None (context, cache)
    let createLatestKnownEvent codec initial fold (context, cache) =
        createCached codec initial fold (Some Equinox.EventStoreDb.AccessStrategy.LatestKnownEvent) (context, cache)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Context<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
    | Esdb of   Equinox.EventStoreDb.EventStoreContext * Equinox.Core.ICache
