/// Application Service that controls the ingestion of tickets into a chain of `TicketsEpoch` streams per FC
/// - the `TicketsSeries` aggregate maintains a pointer to the current Epoch for each FC
/// - as `TicketEpoch`s complete (have `Closed` events logged), we update the `active` Epoch in the TicketsSeries
///     to reference the new one
/// - Ingestion deduplicates on a best-effort basis looking back a predefined number of epochs.
///   - Re-ingestion of items prior to this window is possible (implying the feed can potentially serve duplicates,
///     which the feedConsumer is expected to handle idempotently).
///   - Note also that the IdsCache is presently unbounded
/// TODO please consider using the ListIngester pattern https://github.com/jet/dotnet-templates/pull/94 instead unless you are snookered
///      i.e. you have no meaningful way to stash a RegistrationEpoch attached to the source information in order to make things more
///           deterministic in nature
module FeedSourceTemplate.Domain.TicketsIngester

open System.Threading
open Equinox.Core
open FSharp.UMX

type internal IdsCache() =
    // NOTE: Bounded only by relatively low number of physical pick tickets IRL
    let all = System.Collections.Generic.HashSet<TicketId>()
    static member Create init = let x = IdsCache() in x.Add init; x
    member _.Add tickets = all.UnionWith tickets
    member _.Contains ticket = all.Contains ticket

/// Maintains active EpochId in a thread-safe manner while ingesting items into the chain of `epochs` indexed by the `series`
/// Prior to first add, reads `lookBack` batches to seed the cache, in order to minimize the number of duplicated tickets we ingest
type ServiceForFc internal (log : Serilog.ILogger, fcId, epochs : TicketsEpoch.IngestionService, series : TicketsSeries.Service, lookBack, linger) =

    // Maintains what we believe to be the currently open EpochId.
    // NOTE not valid/initialized until invocation of `previousTicket.AwaitValue()` has completed
    let uninitializedSentinel = %TicketsEpochId.unknown
    let mutable activeEpochId = uninitializedSentinel
    let effectiveEpochId () = if activeEpochId = uninitializedSentinel then TicketsEpochId.initial else %activeEpochId

    // establish the pre-existing items from which the tickets cache will be seeded
    let loadPreviousEpochs loadDop : Async<TicketId[][]> = async {
        match! series.TryReadIngestionEpochId fcId with
        | None ->
            log.Information("No starting epoch registered for {fcId}", fcId)
            return Array.empty
        | Some startingId ->
            log.Information("Walking back from {fcId}/{epochId}", fcId, startingId)
            activeEpochId <- %startingId
            let readEpoch epochId =
                log.Information("Reading {fcId}/{epochId}", fcId, epochId)
                epochs.ReadTickets(fcId, epochId)
            return! Async.Parallel(seq { for epochId in (max 0 (%startingId - lookBack)) .. (%startingId - 1) -> readEpoch %epochId }, loadDop) }

    // Tickets cache - used to maintain a list of tickets that have already been ingested in order to avoid db round-trips
    let previousTickets : AsyncCacheCell<IdsCache> =
        let aux = async {
            let! batches = loadPreviousEpochs 4
            return IdsCache.Create(Seq.concat batches) }
        AsyncCacheCell(fun ct -> Async.StartAsTask(aux, cancellationToken = ct))

    let tryIngest items = async {
        let! ct = Async.CancellationToken
        let! previousTickets = previousTickets.Await ct |> Async.AwaitTask
        let firstEpochId = effectiveEpochId ()

        let rec aux epochId ingestedTickets items = async {
            let dup, freshItems = items |> Array.partition (TicketsEpoch.itemId >> previousTickets.Contains)
            let fullCount = Array.length items
            let dropping = fullCount - Array.length freshItems
            if dropping <> 0 then
                log.Information("Ignoring {count}/{fullCount} duplicate ids: {ids} for {fcId}/{epochId}", dropping, fullCount, dup, fcId, epochId)
            if Array.isEmpty freshItems then
                return ingestedTickets
            else
                let! res = epochs.Ingest(fcId, epochId, freshItems)
                let ingestedTickets = Array.append ingestedTickets res.accepted
                if (not << Array.isEmpty) res.accepted then
                    log.Information("Added {count} items to {fcId:l}/{epochId}", res.accepted.Length, fcId, epochId)
                // The adding is potentially redundant; we don't care
                previousTickets.Add res.content
                // Any writer noticing we've moved to a new epoch shares the burden of marking it active
                if not res.closed && activeEpochId < TicketsEpochId.value epochId then
                    log.Information("Marking {fcId:l}/{epochId} active", fcId, epochId)
                    do! series.MarkIngestionEpochId(fcId, epochId)
                    System.Threading.Interlocked.CompareExchange(&activeEpochId, %epochId, activeEpochId) |> ignore
                match res.residual with
                | [||] -> return ingestedTickets
                | remaining -> return! aux (TicketsEpochId.next epochId) ingestedTickets remaining }
        return! aux firstEpochId [||] (Array.concat items)
    }

    /// Within the processing for a given FC, we have a Scheduler running N streams concurrently
    /// If each thread works in isolation, they'll conflict with each other as they feed the ticket into the batch in epochs.Ingest
    /// Instead, we enable concurrent requests to coalesce by having requests converge in this AsyncBatchingGate
    /// This has the following critical effects:
    /// - Traffic to CosmosDB is naturally constrained to a single flight in progress
    ///   (BatchingGate does not release next batch for execution until current has succeeded or throws)
    /// - RU consumption for writing to the batch is optimized (1 write inserting 1 event document vs N writers writing N)
    /// - Peak throughput is more consistent as latency is not impacted by the combination of having to:
    ///   a) back-off, re-read and retry if there's a concurrent write Optimistic Concurrency Check failure when writing the stream
    ///   b) enter a prolonged period of retries if multiple concurrent writes trigger rate limiting and 429s from CosmosDB
    ///   c) readers will less frequently encounter sustained 429s on the batch
    let batchedIngest = AsyncBatchingGate(tryIngest, linger)

    /// Upon startup, we initialize the Tickets cache from recent epochs; we want to kick that process off before our first ingest
    member _.Initialize() = async {
        let! ct = Async.CancellationToken
        return! previousTickets.Await(ct) |> Async.AwaitTask |> Async.Ignore }

    /// Attempts to feed the items into the sequence of epochs. Returns the subset that actually got fed in this time around.
    member _.IngestMany(items : TicketsEpoch.Events.Item[]) : Async<TicketId[]> = async {
        let! results = batchedIngest.Execute items
        return System.Linq.Enumerable.Intersect(Seq.map TicketsEpoch.itemId items, results) |> Array.ofSeq
    }

    /// Attempts to feed the item into the sequence of batches. Returns true if the item actually got included into an Epoch this time around.
    member _.TryIngest(item : TicketsEpoch.Events.Item) : Async<bool> = async {
        let! result = batchedIngest.Execute(Array.singleton item)
        return result |> Array.contains (TicketsEpoch.itemId item)
    }

let private createFcService (epochs, lookBackLimit) series linger fcId =
    let log = Serilog.Log.ForContext<ServiceForFc>()
    ServiceForFc(log, fcId, epochs, series, lookBack=lookBackLimit, linger=linger)

/// Each ServiceForFc maintains significant state (set of tickets looking back through e.g. 100 epochs), which we obv need to cache
type Service internal (createForFc : FcId -> ServiceForFc) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let forFc = System.Collections.Concurrent.ConcurrentDictionary<FcId, Lazy<ServiceForFc>>()
    let build fcId = lazy createForFc fcId

    member _.ForFc(fcId) : ServiceForFc =
        forFc.GetOrAdd(fcId, build).Value

type Config() =

    static let create linger maxItemsPerEpoch lookBackLimit store =
        let remainingBatchCapacity _candidateItems currentItems =
            let l = Array.length currentItems
            max 0 (maxItemsPerEpoch - l)
        let epochs = TicketsEpoch.Config.create remainingBatchCapacity store
        let series = TicketsSeries.Config.create None store
        let createForFc = createFcService (epochs, lookBackLimit) series linger
        Service createForFc

    static member Create(store, ?linger, ?maxItemsPerEpoch, ?lookBackLimit) =
        let maxItemsPerEpoch, lookBackLimit = defaultArg maxItemsPerEpoch 50_000, defaultArg lookBackLimit 100
        let linger = defaultArg linger (System.TimeSpan.FromMilliseconds 200.)
        create linger maxItemsPerEpoch lookBackLimit store
