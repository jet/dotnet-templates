namespace Shipping.Domain

open FSharp.UMX

[<Measure>] type shipmentId
type ShipmentId = string<shipmentId>
module ShipmentId =
    let toString (x : ShipmentId) : string = %x

[<Measure>] type containerId
type ContainerId = string<containerId>
module ContainerId =
    let toString (x : ContainerId) : string = %x

[<Measure>] type transactionId
type TransactionId = string<transactionId>
module TransactionId =
    let toString (x : TransactionId) : string = %x
    let parse (x : string) = %x
    let (|Parse|) = parse

namespace global

module Config =

    /// Tag log entries so we can filter them out if logging to the console
    let log = Serilog.Log.ForContext("isMetric", true)
    let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)

    module Category =

        let createMemory codec initial fold store =
            Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)

        let private create codec initial fold accessStrategy (context, cache) =
            let cacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
            Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, cacheStrategy, accessStrategy)

        let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
            let accessStrategy = Equinox.CosmosStore.AccessStrategy.Snapshot (isOrigin, toSnapshot)
            create codec initial fold accessStrategy (context, cache)

    [<NoComparison; NoEquality; RequireQualifiedAccess>]
    type Store<'t> =
        | Memory of Equinox.MemoryStore.VolatileStore<'t>
        | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
