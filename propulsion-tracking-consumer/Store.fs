module ConsumerTemplate.Store

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider cat = Equinox.Decider.resolve log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() // options = Options.Default

module Cosmos =

    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Context =
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Cache
