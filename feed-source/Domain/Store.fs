module FeedSourceTemplate.Domain.Store

module Metrics = 

    let log = Serilog.Log.ForContext("isMetric", true)

let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() |> FsCodec.SystemTextJson.Encoder.Compressed // options = Options.Default

module Memory =

    let create name codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, name, codec, fold, initial)

module Cosmos =

    let private createCached name codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

    let createUnoptimized name codec initial fold (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Unoptimized
        createCached name codec initial fold accessStrategy (context, cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Config<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Cache
