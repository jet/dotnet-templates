module TodoBackendTemplate.Config

let log = Serilog.Log.ForContext("isMetric", true)
let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)

module Category =

    let createMemory _codec initial fold store =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial)

    let private createCosmos codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

    let createCosmosSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCosmos codec initial fold accessStrategy (context, cache)

    let createCosmosRollingState codec initial fold toSnapshot (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.RollingState toSnapshot
        createCosmos codec initial fold accessStrategy (context, cache)

    let createEsdbSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = Equinox.EventStore.AccessStrategy.RollingSnapshots (isOrigin, toSnapshot)
        Equinox.EventStore.EventStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
#if (memoryStore || (!cosmos && !eventStore))
type Store<'t> =
    | Memory of Equinox.MemoryStore.VolatileStore<'t>
#else
type Store =
#endif
//#if cosmos
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
//#endif
//#if eventStore
    | Esdb of Equinox.EventStore.EventStoreContext * Equinox.Core.ICache
//#endif
