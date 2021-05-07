/// Application Service that controls the ingestion of Items into a chain of `Epoch` streams per Tranche
/// - the `Series` aggregate maintains a pointer to the current Epoch for each Tranche
/// - as `Epoch`s complete (have `Closed` events logged), we update the `active` Epoch in the Series to reference the new one
module Patterns.Domain.ItemIngester

open Equinox.Core
open FSharp.UMX

type internal IdsCache() =
    // NOTE: Bounded only by relatively low number of physical items IRL
    let all = System.Collections.Generic.HashSet<ItemId>()
    static member Create init = let x = IdsCache() in x.Add init; x
    member _.Add ids = all.UnionWith ids
    member _.Contains id = all.Contains id

/// Maintains active EpochId in a thread-safe manner while ingesting items into the chain of `epochs` indexed by the `series`
/// Prior to first add, reads `lookBack` batches to seed the cache, in order to minimize the number of duplicated items we ingest
type ServiceForTranche internal (log : Serilog.ILogger, trancheId, epochs : ItemEpoch.IngestionService, series : ItemSeries.Service, lookBack, linger) =

    // Maintains what we believe to be the currently open EpochId
    // NOTE not valid/initialized until invocation of `previousIds.AwaitValue()` has completed
    let uninitializedSentinel = %ItemEpochId.unknown
    let mutable activeEpochId_ = uninitializedSentinel
    // NOTE see above - must not be called prior to previousIds.AwaitValue()
    let effectiveEpochId () = if activeEpochId_ = uninitializedSentinel then ItemEpochId.initial else %activeEpochId_

    // establish the pre-existing items from which the previousIds cache will be seeded
    let loadPreviousEpochs loadDop : Async<ItemId[][]> = async {
        match! series.TryReadIngestionEpochId trancheId with
        | None ->
            log.Information("No starting epoch registered for {trancheId}", trancheId)
            return Array.empty
        | Some startingId ->
            log.Information("Walking back from {trancheId}/{epochId}", trancheId, startingId)
            activeEpochId_ <- %startingId
            let readEpoch epochId =
                log.Information("Reading {trancheId}/{epochId}", trancheId, epochId)
                epochs.ReadIds(trancheId, epochId)
            return! Async.Parallel(seq { for epochId in (max 0 (%startingId - lookBack)) .. (%startingId - 1) -> readEpoch %epochId }, loadDop) }

    // ItemIds cache - used to maintain a list of items that have already been ingested in order to avoid db round-trips
    let previousIds : AsyncCacheCell<IdsCache> = AsyncCacheCell <| async {
        let! batches = loadPreviousEpochs 4
        return IdsCache.Create(Seq.concat batches) }

    let tryIngest items = async {
        let! previousIds = previousIds.AwaitValue()
        let firstEpochId = effectiveEpochId ()

        let rec aux epochId ingestedItems items = async {
            let dup, freshItems = items |> Array.partition (ItemEpoch.itemId >> previousIds.Contains)
            let fullCount = Array.length items
            let dropping = fullCount - Array.length freshItems
            if dropping <> 0 then
                log.Information("Ignoring {count}/{fullCount} duplicate ids: {ids} for {trancheId}/{epochId}", dropping, fullCount, dup, trancheId, epochId)
            if Array.isEmpty freshItems then
                return ingestedItems
            else
                let! res = epochs.Ingest(trancheId, epochId, freshItems)
                let ingestedItemIds = Array.append ingestedItems res.accepted
                if (not << Array.isEmpty) res.accepted then
                    log.Information("Added {count} items to {trancheId}/{epochId}", res.accepted.Length, trancheId, epochId)
                // The adding is potentially redundant; we don't care
                previousIds.Add res.content
                // Any writer noticing we've moved to a new Epoch shares the burden of marking it active in the Series
                if not res.closed && activeEpochId_ < ItemEpochId.value epochId then
                    log.Information("Marking {trancheId}/{epochId} active", trancheId, epochId)
                    do! series.MarkIngestionEpochId(trancheId, epochId)
                    System.Threading.Interlocked.CompareExchange(&activeEpochId_, %epochId, activeEpochId_) |> ignore
                match res.residual with
                | [||] -> return ingestedItemIds
                | remaining -> return! aux (ItemEpochId.next epochId) ingestedItemIds remaining }
        return! aux firstEpochId [||] (Array.concat items)
    }

    /// Within the processing for a given Tranche, we have a Scheduler running N streams concurrently
    /// If each thread works in isolation, they'll conflict with each other as they feed the ticket into the batch in epochs.Ingest
    /// Instead, we enable concurrent requests to coalesce by having requests converge in this AsyncBatchingGate
    /// This has the following critical effects:
    /// - Traffic to CosmosDB is naturally constrained to a single flight in progress
    ///   (BatchingGate does not admit next batch until current has succeeded or throws)
    /// - RU consumption for writing to the batch is optimized (1 write inserting 1 event document vs N writers writing N)
    /// - Peak throughput is more consistent as latency is not impacted by the combination of having to:
    ///   a) back-off, re-read and retry if there's a concurrent write Optimistic Concurrency Check failure when writing the stream
    ///   b) enter a prolonged period of retries if multiple concurrent writes trigger rate limiting and 429s from CosmosDB
    ///   c) readers will less frequently encounter sustained 429s on the batch
    let batchedIngest = AsyncBatchingGate(tryIngest, linger)

    /// Upon startup, we initialize the ItemIds cache from recent epochs; we want to kick that process off before our first ingest
    member _.Initialize() = previousIds.AwaitValue() |> Async.Ignore

    /// Attempts to feed the items into the sequence of epochs.
    /// Returns the subset that actually got fed in this time around.
    member _.IngestMany(items : ItemEpoch.Events.Item[]) : Async<ItemId seq> = async {
        let! results = batchedIngest.Execute items
        return System.Linq.Enumerable.Intersect(Seq.map ItemEpoch.itemId items, results)
    }

    /// Attempts to feed the item into the sequence of batches.
    /// Returns true if the item actually got included into an Epoch this time around.
    member _.TryIngest(item : ItemEpoch.Events.Item) : Async<bool> = async {
        let! result = batchedIngest.Execute(Array.singleton item)
        return result |> Array.contains (ItemEpoch.itemId item)
    }

let private createServiceForTranche (epochs, lookBackLimit) series linger trancheId =
    let log = Serilog.Log.ForContext<ServiceForTranche>()
    ServiceForTranche(log, trancheId, epochs, series, lookBack=lookBackLimit, linger=linger)

/// Each ServiceForTranche maintains significant state (set of itemIds looking back through e.g. 100 epochs), which we obv need to cache
type Service internal (createForTranche : ItemTrancheId -> ServiceForTranche) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forTranche = System.Collections.Concurrent.ConcurrentDictionary<ItemTrancheId, Lazy<ServiceForTranche>>()
    let build trancheId = lazy createForTranche trancheId

    member _.ForTranche trancheId : ServiceForTranche =
        forTranche.GetOrAdd(trancheId, build).Value

let private maxItemsPerEpoch = 10_000
let private linger, lookBackLimit = System.TimeSpan.FromMilliseconds 200., 100

type MemoryStore() =

    static member Create(store, linger, maxItemsPerEpoch, lookBackLimit) =
        let remainingBatchCapacity _candidateItems currentItems =
            let l = Array.length currentItems
            max 0 (maxItemsPerEpoch - l)
        let epochs = ItemEpoch.MemoryStore.create remainingBatchCapacity store
        let series = ItemSeries.MemoryStore.create store
        let createForTranche = createServiceForTranche (epochs, lookBackLimit) series linger
        Service createForTranche

module Cosmos =

    let create (context, cache) =
        let remainingBatchCapacity _candidateItems currentItems =
            let l = Array.length currentItems
            max 0 (maxItemsPerEpoch - l)
        let epochs = ItemEpoch.Cosmos.create remainingBatchCapacity (context, cache)
        let series = ItemSeries.Cosmos.create (context, cache)
        let createForTranche = createServiceForTranche (epochs, lookBackLimit) series linger
        Service createForTranche
