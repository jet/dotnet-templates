/// Application Service that controls the ingestion of Items into a chain of `Epoch` streams
/// - the `Series` aggregate maintains a pointer to the current Epoch
/// - as `Epoch`s complete (have `Closed` events logged), we update the `active` Epoch in the Series to reference the new one
/// The fact that each request walks forward from a given start point until it either gets to append (or encounters a prior insertion)
///   means we can guarantee the insertion/deduplication to be idempotent and insert exactly once per completed execution
module Patterns.Domain.ItemIngester

open FSharp.UMX // %

type Service internal (log : Serilog.ILogger, epochs : ItemEpoch.Service, series : ItemSeries.Service, linger) =

    let uninitializedSentinel : int = %ItemEpochId.unknown
    let mutable currentEpochId_ = uninitializedSentinel
    let currentEpochId () = if currentEpochId_ = uninitializedSentinel then Some %currentEpochId_ else None
    
    let tryIngest (items : (ItemEpochId * ItemId)[][]) = async {
        let rec aux ingestedItems items = async {
            let epochId = items |> Array.minBy fst |> fst
            let epochItems, futureEpochItems = items |> Array.partition (fun (e,_) -> e = epochId)
            let! res = epochs.Ingest(epochId, Array.map snd epochItems)
            let ingestedItemIds = Array.append ingestedItems res.accepted
            if (not << Array.isEmpty) res.accepted then
                log.Information("Added {count}/{total} items to {epochId} Residual {residual} Future {future}",
                                res.accepted.Length, epochItems.Length, epochId, futureEpochItems.Length)
            let nextEpochId = ItemEpochId.next epochId
            let pushedToNextEpoch = res.residual |> Array.map (fun x -> nextEpochId, x)
            match Array.append pushedToNextEpoch futureEpochItems with
            | [||] ->
                // Any writer noticing we've moved to a new Epoch shares the burden of marking it active in the Series
                let newActiveEpochId = if res.closed then nextEpochId else epochId
                if currentEpochId_ < %newActiveEpochId then
                    log.Information("Marking {epochId} active", newActiveEpochId)
                    do! series.MarkIngestionEpochId(newActiveEpochId)
                    System.Threading.Interlocked.CompareExchange(&currentEpochId_, %newActiveEpochId, currentEpochId_) |> ignore
                return ingestedItemIds
            | remaining -> return! aux ingestedItemIds remaining }
        return! aux [||] (Array.concat items)
    }

    /// In the overall processing using an Ingester, we frequently have a Scheduler running N streams concurrently
    /// If each thread works in isolation, they'll conflict with each other as they feed the Items into the batch in epochs.Ingest
    /// Instead, we enable concurrent requests to coalesce by having requests converge in this AsyncBatchingGate
    /// This has the following critical effects:
    /// - Traffic to CosmosDB is naturally constrained to a single flight in progress
    ///   (BatchingGate does not release next batch for execution until current has succeeded or throws)
    /// - RU consumption for writing to the batch is optimized (1 write inserting 1 event document vs N writers writing N)
    /// - Peak throughput is more consistent as latency is not impacted by the combination of having to:
    ///   a) back-off, re-read and retry if there's a concurrent write Optimistic Concurrency Check failure when writing the stream
    ///   b) enter a prolonged period of retries if multiple concurrent writes trigger rate limiting and 429s from CosmosDB
    ///   c) readers will less frequently encounter sustained 429s on the batch
    let batchedIngest = Equinox.Core.AsyncBatchingGate(tryIngest, linger)

    /// Determines the current active epoch
    /// Uses cached values as epoch transitions are rare, and caller needs to deal with the inherent race condition in any case
    member _.CurrentIngestionEpochId() : Async<ItemEpochId> =
        match currentEpochId () with
        | Some currentEpochId -> async { return currentEpochId }
        | None -> series.ReadIngestionEpochId()

    /// Attempts to feed the items into the sequence of epochs.
    /// Returns the subset that actually got fed in this time around.
    member _.IngestMany(epochId, items) : Async<ItemId seq> = async {
        if Array.isEmpty items then return Seq.empty else
            
        let! results = batchedIngest.Execute [| for x in items -> epochId, x |]
        return System.Linq.Enumerable.Intersect(items, results)
    }

    /// Attempts to feed the item into the sequence of batches.
    /// Returns true if the item actually got included into an Epoch this time around.
    member _.TryIngest(startEpoch, itemId) : Async<bool> = async {
        let! result = batchedIngest.Execute [| startEpoch, itemId |]
        return result |> Array.contains itemId
    }

let private create epochs series linger =
    let log = Serilog.Log.ForContext<Service>()
    Service(log, epochs, series, linger=linger)

let private maxItemsPerEpoch = 10_000
let private linger = System.TimeSpan.FromMilliseconds 200.

type MemoryStore() =

    static member Create(store, linger, maxItemsPerEpoch) =
        let shouldClose candidateItems currentItems = Array.length currentItems + Array.length candidateItems >= maxItemsPerEpoch
        let epochs = ItemEpoch.MemoryStore.create shouldClose store
        let series = ItemSeries.MemoryStore.create store
        create epochs series linger

module Cosmos =

    let create (context, cache) =
        let shouldClose candidateItems currentItems = Array.length currentItems + Array.length candidateItems >= maxItemsPerEpoch
        let epochs = ItemEpoch.Cosmos.create shouldClose (context, cache)
        let series = ItemSeries.Cosmos.create (context, cache)
        create epochs series linger
