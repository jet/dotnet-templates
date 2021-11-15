module ReactorTemplate.Config

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)

module Category =

    let private createCosmos codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createCosmosSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCosmos codec initial fold accessStrategy (context, cache)

    let createCosmosRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCosmos codec initial fold accessStrategy (context, cache)

//#if multiSource
    let createEsdb codec initial fold (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.EventStore.Resolver(context, codec, fold, initial, cacheStrategy)

//#endif
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Store =
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
//#if multiSource
    | Esdb of Equinox.EventStore.Context * Equinox.Core.ICache
//#endif
