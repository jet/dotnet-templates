/// Application Service that controls the ingestion of tickets into a chain of `TicketsEpoch` streams per FC
/// - the `TicketsSeries` aggregate maintains a pointer to the current Epoch for each FC
/// - as `TicketEpoch`s complete (have `Closed` events logged), we update the `active` Epoch in the TicketsSeries
///     to reference the new one
module FeedApiTemplate.Domain.Tickets

open Equinox.Core
open FSharp.UMX

type internal TicketsCache() =
    // NOTE: Bounded only by relatively low number of physical pick tickets IRL
    let all = System.Collections.Concurrent.ConcurrentDictionary<TicketId,unit>()
    static member Create init = let x = TicketsCache() in x.Add init; x
    member _.Add tickets = for x in tickets do all.[x] <- ()
    member _.Contains ticket = all.ContainsKey ticket

/// Maintains active EpochId in a thread-safe manner while ingesting items into the chain of `epochs` indexed by the `series`
/// Prior to first add, reads `lookBack` batches to seed the cache, in order to minimize the number of duplicated tickets we ingest
type ServiceForFc internal (log : Serilog.ILogger, fcId, epochs : TicketsEpoch.Service, series : TicketsSeries.Service, lookBack, linger) =

    // Maintains what we believe to be the currently open EpochId.
    // NOTE not valid/initialized until invocation of `previousTicket.AwaitValue()` has completed
    let uninitializedSentinel = %TicketsEpochId.unknown
    let mutable activeEpochId = uninitializedSentinel
    let effectiveEpochId () = if activeEpochId = uninitializedSentinel then TicketsEpochId.initial else %activeEpochId

    // We want max one request in flight to establish the pre-existing items from which the tickets cache will be seeded
    let previousEpochs = AsyncCacheCell<AsyncCacheCell<TicketId[]> list> <| async {
        match! series.TryReadIngestionEpochId fcId with
        | None ->
            log.Information("No starting epoch registered for {fcId}", fcId)
            return []
        | Some startingId ->
            log.Information("Walking back from {fcId}/{epochId}", fcId, startingId)
            activeEpochId <- %startingId
            let readEpoch epochId =
                log.Information("Reading {fcId}/{epochId}", fcId, epochId)
                epochs.ReadTickets(fcId, epochId)
            return [ for epochId in (max 0 (%startingId - lookBack)) .. (%startingId - 1) -> AsyncCacheCell(readEpoch %epochId) ] }

    // Tickets cache - used to maintain a list of tickets that have already been ingested in order to avoid db round-trips
    let previousTickets : AsyncCacheCell<TicketsCache> = AsyncCacheCell <| async {
        let! batches = previousEpochs.AwaitValue()
        let! tickets = seq { for x in batches -> x.AwaitValue() } |> Async.Parallel
        return TicketsCache.Create(Seq.concat tickets) }

    let tryIngest items = async {
        let! previousTickets = previousTickets.AwaitValue()
        let firstEpochId = effectiveEpochId ()

        let rec aux epochId ingestedTickets items = async {
            let dup, fresh = items |> Array.partition previousTickets.Contains
            let fullCount = Array.length items
            let dropping = fullCount - Array.length fresh
            if dropping <> 0 then
                log.Information("Ignoring {count}/{fullCount} duplicate ids: {ids} for {fcId}/{epochId}", dropping, fullCount, dup, fcId, epochId)
            if Array.isEmpty fresh then
                return ingestedTickets
            else
                let! res = epochs.Ingest(fcId, epochId, fresh)
                let ingestedTickets = Array.append ingestedTickets res.added
                if (not << Array.isEmpty) res.added then
                    log.Information("Added {count} items to {fcId:l}/{epochId}", res.added.Length, fcId, epochId)
                // The adding is potentially redundant; we don't care
                previousTickets.Add res.content
                // Any writer noticing we've moved to a new epoch shares the burden of marking it active
                if not res.isClosed && activeEpochId < TicketsEpochId.value epochId then
                    log.Information("Marking {fcId:l}/{epochId} active", fcId, epochId)
                    do! series.MarkIngestionEpochId(fcId, epochId)
                    System.Threading.Interlocked.CompareExchange(&activeEpochId, %epochId, activeEpochId) |> ignore
                match res.rejected with
                | [||] -> return ingestedTickets
                | remaining -> return! aux (TicketsEpochId.next epochId) ingestedTickets remaining }
        return! aux firstEpochId [||] items
    }

    /// Within the processing for a given FC, we have a Scheduler running N streams concurrently
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

    /// Upon startup, we initialize the Tickets cache from recent epochs; we want to kick that process off before our first ingest
    member _.Initialize() = previousEpochs.AwaitValue() |> Async.Ignore

    /// Attempts to feed the items into the sequence of epochs. Returns the subset that actually got fed in this time around.
    member _.IngestMany(items : TicketId[]) : Async<TicketId seq> = async {
        let! results = items |> Seq.map batchedIngest.Execute |> Async.Parallel
        return (Seq.collect id results, items) |> System.Linq.Enumerable.Intersect
    }

    /// Attempts to feed the item into the sequence of batches. Returns true if the item actually got included into an Epoch this time around.
    member _.TryIngest(item : TicketId) : Async<bool> = async {
        let! result = batchedIngest.Execute item
        return result |> Array.contains item
    }

let private createFcService (epochs, lookBackLimit) series linger fcId =
    let log = Serilog.Log.ForContext<ServiceForFc>().ForContext("fcId", fcId)
    ServiceForFc(log, fcId, epochs, series, lookBack=lookBackLimit, linger=linger)

/// Each ServiceForFc maintains significant state (set of tickets looking back through e.g. 100 epochs), which we obv need to cache
type Service internal (createForFc : FcId -> ServiceForFc) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forFc = System.Collections.Concurrent.ConcurrentDictionary<FcId, Lazy<ServiceForFc>>()
    let build fcId = lazy createForFc fcId

    member _.ForFc(fcId) : ServiceForFc =
        forFc.GetOrAdd(fcId, build).Value

module Cosmos =

    let create (context, cache) =
        let maxTicketsPerEpoch, lookBackLimit = 50_000, 100
        let epochs = TicketsEpoch.Cosmos.createIngester maxTicketsPerEpoch (context, cache)
        let series = TicketsSeries.Cosmos.create (context, cache)
        let linger = System.TimeSpan.FromMilliseconds 200.
        let createForFc = createFcService (epochs, lookBackLimit) series linger
        Service(createForFc)
