module Shipping.Domain.Config

/// Tag log entries so we can filter them out if logging to the console
let log = Serilog.Log.ForContext("isMetric", true)

module EventCodec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create(autoTypeSafeEnumToJsonString = true, autoUnionToJsonObject = true)
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions) |> FsCodec.Deflate.EncodeTryDeflate
    let genUncompressed<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions) |> FsCodec.Deflate.EncodeUncompressed
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        CodecJsonElement.Create<'t>(options = defaultOptions)

module Memory =

    let create codec initial fold store =
        Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)

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
        DynamoStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Store<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
