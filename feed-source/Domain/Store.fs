module FeedSourceTemplate.Domain.Store

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider cat = Equinox.Decider.resolve log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() // options = Options.Default

module Memory =

    let create codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)

module Cosmos =

    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Unoptimized
        createCached codec initial fold accessStrategy (context, cache)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Context<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
