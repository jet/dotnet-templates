module Domain.Store

let log = Serilog.Log.ForContext("isMetric", true)
let resolve cat = Equinox.Decider.resolve log cat

module Codec =

    open FsCodec.SystemTextJson

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>() // options = Options.Default

module Memory =

    let create codec initial fold store: Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Deflate.EncodeUncompressed codec, fold, initial)

let defaultCacheDuration = System.TimeSpan.FromMinutes 20.

module Dynamo =

    open Equinox.DynamoStore

    let private create codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, cacheStrategy, accessStrategy)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create codec initial fold accessStrategy (context, cache)

module Mdb =

    open Equinox.MessageDb

    let private create codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        MessageDbCategory(context, codec, fold, initial, cacheStrategy, ?access = accessStrategy)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = None
        create codec initial fold accessStrategy (context, cache)

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Context =
    | Memory of Equinox.MemoryStore.VolatileStore<struct (int * System.ReadOnlyMemory<byte>)>
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
    | Mdb    of Equinox.MessageDb.MessageDbContext * Equinox.Core.ICache
