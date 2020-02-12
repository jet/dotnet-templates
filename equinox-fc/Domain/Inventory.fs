namespace Domain.Inventory

open Equinox.Core // we use Equinox's AsyncCacheCell helper below
open FSharp.UMX

type internal TicketsCache() =
    let all = System.Collections.Concurrent.ConcurrentDictionary<TransferId,unit>() // Bounded only by relatively low number of physical pick tickets IRL
    static member Create init = let x = TicketsCache() in x.Add init; x
    member __.Add tickets = for x in tickets do all.[x] <- ()
    member __.Contains ticket = all.ContainsKey ticket

/// Maintains active BatchId in a thread-safe manner while ingesting items into the chain of `batches` indexed by the `transmissions`
/// Prior to first add, reads `lookBack` batches to seed the cache, in order to minimize the number of duplicated pickTickets we transmit
type Ingester internal (fcId, batches : Batch.Service, transmissions : Transmissions.Service, lookBack, capacity) =

    let log = Serilog.Log.ForContext<Ingester>()
    // Maintains what we believe to be the currently open BatchId.
    // Guaranteed to be set only after `previousTicket.AwaitValue()`
    let mutable activeId = Unchecked.defaultof<_>

    // We want max one request in flight to establish the pre-existing Batches from which the tickets cache will be seeded
    let previousBatches = AsyncCacheCell<AsyncCacheCell<PickTicketId list> list> <| async {
        let! startingId = transmissions.ReadIngestionBatchId(fcId)
        activeId <- %startingId
        let readBatch batchId = async { let! r = batches.IngestShipped(fcId,batchId,(fun _ -> 1),Seq.empty) in return r.batchContent }
        return [ for b in (max 0 (%startingId - lookBack)) .. (%startingId - 1) -> AsyncCacheCell(readBatch %b) ] }

    // Tickets cache - used to maintain a list of tickets that have already been ingested in order to avoid db round-trips
    let previousTickets : AsyncCacheCell<TicketsCache> = AsyncCacheCell <| async {
        let! batches = previousBatches.AwaitValue()
        let! tickets = seq { for x in batches -> x.AwaitValue() } |> Async.Parallel
        return TicketsCache.Create(Seq.concat tickets) }

    let tryIngest items = async {
        let! previousTickets = previousTickets.AwaitValue()
        let firstBatchId = %activeId

        let rec aux batchId totalIngestedTickets items = async {
            let dup,fresh = items |> List.partition previousTickets.Contains
            let fullCount = List.length items
            let dropping = fullCount - List.length fresh
            if dropping <> 0 then log.Information("Ignoring {count}/{fullCount} duplicate ids: {ids} for {batchId}", dropping, fullCount, dup, batchId)
            if List.isEmpty fresh then
                return totalIngestedTickets
            else
                let! res = batches.IngestShipped(fcId,batchId,capacity,fresh)
                log.Information("Added {count} items to {fcId:l}/{batchId}", res.added, fcId, batchId)
                // The adding is potentially redundant; we don't care
                previousTickets.Add res.batchContent
                // Any writer noticing we've moved to a new batch shares the burden of marking it active
                if not res.isClosed && activeId < %batchId then
                    log.Information("Marking {fcId:l}/{batchId} active", fcId, batchId)
                    do! transmissions.MarkIngestionBatchId(fcId,batchId)
                    System.Threading.Interlocked.CompareExchange(&activeId,%batchId,activeId) |> ignore
                let totalIngestedTickets = totalIngestedTickets + res.added
                match res.rejected with
                | [] -> return totalIngestedTickets
                | rej -> return! aux (BatchId.next batchId) totalIngestedTickets rej }
        return! aux firstBatchId 0 items
    }

    /// Upon startup, we initialize the PickTickets cache with recent batches; we want to kick that process off before our first ingest
    member __.Initialize() = previousBatches.AwaitValue() |> Async.Ignore

    /// Feeds the items into the sequence of batches. Returns the number of items actually added [excluding duplicates]
    member __.Ingest(items : PickTicketId list) : Async<int> = tryIngest items

module PickTicketsIngester =

    let create fcId maxPickTicketsPerBatch lookBackLimit batchesResolve (epochOverride,transmissionsResolve) =
        let remainingBatchCapacity (state: Financial.Batch.Fold.State) =
            let l = state.ItemCount
            max 0 (maxPickTicketsPerBatch-l)
        let batches = Financial.Batch.createService batchesResolve
        let transmissions = Financial.Transmissions.createService epochOverride transmissionsResolve
        Ingester(fcId, batches, transmissions, lookBack=lookBackLimit, capacity=remainingBatchCapacity)

    module Cosmos =

        let create epochOverride fcId maxPickTicketsPerBatch lookBackLimit (context,cache) =
            let batchesResolve = Financial.Batch.Cosmos.resolve (context,cache)
            let transmissionsResolve = Financial.Transmissions.Cosmos.resolve (context,cache)
            create fcId maxPickTicketsPerBatch lookBackLimit batchesResolve (epochOverride,transmissionsResolve)
