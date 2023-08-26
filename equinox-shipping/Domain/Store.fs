module Store

module Metrics = 

    let log = Serilog.Log.ForContext("isMetric", true)

let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create(autoTypeSafeEnumToJsonString = true, autoUnionToJsonObject = true)
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions)
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>(options = defaultOptions)

module Memory =

    let create name codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, name, FsCodec.Deflate.EncodeUncompressed codec, fold, initial)

let private defaultCacheDuration = System.TimeSpan.FromMinutes 20
let private cacheStrategy cache = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)

module Cosmos =

    open Equinox.CosmosStore
    
    let private createCached name codec initial fold accessStrategy (context, cache) =
        CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

module Dynamo =

    open Equinox.DynamoStore
    
    let private createCached name codec initial fold accessStrategy (context, cache) =
        DynamoStoreCategory(context, name, FsCodec.Deflate.EncodeTryDeflate codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

module Esdb =

    let private createCached name codec initial fold accessStrategy (context, cache) =
        let cachingStrategy = cache |> Option.map cacheStrategy |> Option.defaultValue Equinox.CachingStrategy.NoCaching
        Equinox.EventStoreDb.EventStoreCategory(context, name, codec, fold, initial, accessStrategy, cachingStrategy)
    let createUnoptimized name codec initial fold (context, cache) =
        createCached name codec initial fold Equinox.EventStoreDb.AccessStrategy.Unoptimized (context, Some cache)
    let createLatestKnownEvent name codec initial fold (context, cache) =
        createCached name codec initial fold Equinox.EventStoreDb.AccessStrategy.LatestKnownEvent (context, None)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Config<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Cache
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Cache
    | Esdb of   Equinox.EventStoreDb.EventStoreContext * Equinox.Cache
