module Domain.Store

module Metrics = 

    let log = Serilog.Log.ForContext("isMetric", true)

let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>() // options = Options.Default

module Memory =

    let create name codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, name, FsCodec.Deflate.EncodeUncompressed codec, fold, initial)

let private defaultCacheDuration = System.TimeSpan.FromMinutes 20
let private cacheStrategy cache = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)

module Dynamo =

    open Equinox.DynamoStore

    let private create name codec initial fold accessStrategy (context, cache) =
        DynamoStoreCategory(context, name, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createUnoptimized name codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create name codec initial fold accessStrategy (context, cache)

module Mdb =

    open Equinox.MessageDb

    let private create name codec initial fold accessStrategy (context, cache) =
        MessageDbCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createUnoptimized name codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create name codec initial fold accessStrategy (context, cache)

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Config =
    | Memory of Equinox.MemoryStore.VolatileStore<struct (int * System.ReadOnlyMemory<byte>)>
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Cache
    | Mdb    of Equinox.MessageDb.MessageDbContext * Equinox.Cache
