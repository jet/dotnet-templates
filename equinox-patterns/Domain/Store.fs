module Patterns.Domain.Store

module Metrics = 

    let log = Serilog.Log.ForContext("isMetric", true)

let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>() // options = Options.Default
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() // options = Options.Default

module Memory =

    let create name codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, name, FsCodec.Compression.EncodeUncompressed codec, fold, initial)

let private defaultCacheDuration = System.TimeSpan.FromMinutes 20
let private cacheStrategy cache = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)

module Cosmos =

    let private createCached name codec initial fold accessStrategy (context, cache) =
        Equinox.CosmosStore.CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

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
