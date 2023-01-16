module Domain.Config

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Store =
    | Memory of Equinox.MemoryStore.VolatileStore<struct (int * System.ReadOnlyMemory<byte>)>
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache

let log = Serilog.Log.ForContext("isMetric", true)
let resolve cat = Equinox.Decider.resolve log cat

module EventCodec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create()
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions)

module Memory =

    let create codec initial fold store : Equinox.Category<_, _, _> =
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Deflate.EncodeUncompressed codec, fold, initial)

module Dynamo =

    open Equinox.DynamoStore

    let private create codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        DynamoStoreCategory(context, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, cacheStrategy, accessStrategy)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create codec initial fold accessStrategy (context, cache)
