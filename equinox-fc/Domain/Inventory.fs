namespace Fc.Inventory

open Epoch
open Equinox.Core // we use Equinox's AsyncCacheCell helper below
open FSharp.UMX

type internal IdsCache<'Id>() =
    let all = System.Collections.Concurrent.ConcurrentDictionary<'Id, unit>() // Bounded only by relatively low number of physical pick tickets IRL
    static member Create init = let x = IdsCache() in x.Add init; x
    member __.Add ids = for x in ids do all.[x] <- ()
    member __.Contains id = all.ContainsKey id

/// Maintains active Epoch Id in a thread-safe manner while ingesting items into the `series` of `epochs`
/// Prior to first add, reads `lookBack` epochs to seed the cache, in order to minimize the number of duplicated Ids we ingest
type Service2 internal (inventoryId, series : Series.Service, epochs : Epoch.Service, lookBack, capacity) =

    let log = Serilog.Log.ForContext<Service2>()

    // Maintains what we believe to be the currently open EpochId
    // Guaranteed to be set only after `previousIds.AwaitValue()`
    let mutable activeEpochId = Unchecked.defaultof<_>

    // We want max one request in flight to establish the pre-existing Events from which the TransactionIds cache will be seeded
    let previousEpochs = AsyncCacheCell<AsyncCacheCell<Set<InventoryTransactionId>> list> <| async {
        let! startingId = series.ReadIngestionEpoch(inventoryId)
        activeEpochId <- %startingId
        let read epochId = async { let! r = epochs.TryIngest(inventoryId, epochId, (fun _ -> 1), Seq.empty) in return r.transactionIds }
        return [ for epoch in (max 0 (%startingId - lookBack)) .. (%startingId - 1) -> AsyncCacheCell(read %epoch) ] }

    // TransactionIds cache - used to maintain a list of transactions that have already been ingested in order to avoid db round-trips
    let previousIds : AsyncCacheCell<IdsCache<_>> = AsyncCacheCell <| async {
        let! previousEpochs = previousEpochs.AwaitValue()
        let! ids = seq { for x in previousEpochs -> x.AwaitValue() } |> Async.Parallel
        return IdsCache.Create(Seq.concat ids) }

    let tryIngest events = async {
        let! previousIds = previousIds.AwaitValue()
        let initialEpochId = %activeEpochId

        let rec aux epochId totalIngested items = async {
            let SeqPartition f = Seq.toArray >> Array.partition f
            let dup, fresh = items |> SeqPartition (Epoch.Events.chooseInventoryTransactionId >> Option.exists previousIds.Contains)
            let fullCount = List.length items
            let dropping = fullCount - Array.length fresh
            if dropping <> 0 then log.Information("Ignoring {count}/{fullCount} duplicate ids: {ids} for {epochId}", dropping, fullCount, dup, epochId)
            if Array.isEmpty fresh then
                return totalIngested
            else
                let! res = epochs.TryIngest(inventoryId, epochId, capacity, fresh)
                log.Information("Added {count} items to {inventoryId:l}/{epochId}", res.added, inventoryId, epochId)
                // The adding is potentially redundant; we don't care
                previousIds.Add res.transactionIds
                // Any writer noticing we've moved to a new epoch shares the burden of marking it active
                if not res.isClosed && activeEpochId < %epochId then
                    log.Information("Marking {inventoryId:l}/{epochId} active", inventoryId, epochId)
                    do! series.AdvanceIngestionEpoch(inventoryId, epochId)
                    System.Threading.Interlocked.CompareExchange(&activeEpochId, %epochId, activeEpochId) |> ignore
                let totalIngestedTransactions = totalIngested + res.added
                match res.rejected with
                | [] -> return totalIngestedTransactions
                | rej -> return! aux (InventoryEpochId.next epochId) totalIngestedTransactions rej }
        return! aux initialEpochId 0 events
    }

    /// Upon startup, we initialize the TransactionIds cache with recent epochs; we want to kick that process off before our first ingest
    member __.Initialize() = previousIds.AwaitValue() |> Async.Ignore

    /// Feeds the events into the sequence of transactions. Returns the number actually added [excluding duplicates]
    member __.Ingest(events : Epoch.Events.Event list) : Async<int> = tryIngest events

module internal Helpers =

    let create inventoryId maxTransactionsPerEpoch lookBackLimit (series, epochs) =
        let remainingEpochCapacity (state: Epoch.Fold.State) =
            let currentLen = state.ids.Count
            max 0 (maxTransactionsPerEpoch - currentLen)
        Service2(inventoryId, series, epochs, lookBack = lookBackLimit, capacity = remainingEpochCapacity)

module Cosmos =

    let create inventoryId maxTransactionsPerEpoch lookBackLimit (context, cache) =
        let series = Series.Cosmos.create (context, cache)
        let epochs = Epoch.Cosmos.create (context, cache)
        Helpers.create inventoryId maxTransactionsPerEpoch lookBackLimit (series, epochs)

module Processor =

    type Service(transactions : Transaction.Service, locations : Fc.Location.Service, inventory : Service2) =

        let execute transactionId =
            let f = Fc.Location.Epoch.decide transactionId
            let rec aux update = async {
                let! action = transactions.Apply(transactionId, update)
                let aux event = aux (Some event)
                match action with
                | Transaction.Adjust (loc, bal) ->
                    match! locations.Execute(loc, f (Fc.Location.Epoch.Reset bal)) with
                    | Fc.Location.Epoch.Accepted _ -> return! aux Transaction.Events.Adjusted
                    | Fc.Location.Epoch.Denied -> return failwith "Cannot Deny Reset"
                    | Fc.Location.Epoch.DupFromPreviousEpoch -> return failwith "TODO walk back to previous epoch"
                | Transaction.Remove (loc, delta) ->
                    match! locations.Execute(loc, f (Fc.Location.Epoch.Remove delta)) with
                    | Fc.Location.Epoch.Accepted bal -> return! aux (Transaction.Events.Removed { balance = bal })
                    | Fc.Location.Epoch.Denied -> return! aux Transaction.Events.Failed
                    | Fc.Location.Epoch.DupFromPreviousEpoch -> return failwith "TODO walk back to previous epoch"
                | Transaction.Add    (loc, delta) ->
                    match! locations.Execute(loc, f (Fc.Location.Epoch.Add delta)) with
                    | Fc.Location.Epoch.Accepted bal -> return! aux (Transaction.Events.Added   { balance = bal })
                    | Fc.Location.Epoch.Denied -> return failwith "Cannot Deny Add"
                    | Fc.Location.Epoch.DupFromPreviousEpoch -> return failwith "TODO walk back to previous epoch"
                | Transaction.Log (Transaction.Adjusted _) ->
                    let! _count = inventory.Ingest([Fc.Inventory.Epoch.Events.Adjusted    { transactionId = transactionId }])
                    return! aux Transaction.Events.Logged
                | Transaction.Log (Transaction.Transferred _) ->
                    let! _count = inventory.Ingest([Fc.Inventory.Epoch.Events.Transferred { transactionId = transactionId }])
                    return! aux Transaction.Events.Logged
                | Transaction.Finish success ->
                    return success
            }
            aux
        let run transactionId req = execute transactionId (Some req)

        member __.Adjust(transactionId, location, quantity) =
            run transactionId (Fc.Inventory.Transaction.Events.AdjustmentRequested { location = location; quantity = quantity })

        member __.TryTransfer(transactionId, source, destination, quantity) =
            run transactionId (Fc.Inventory.Transaction.Events.TransferRequested { source = source; destination = destination; quantity = quantity })

        /// Used by Watchdog to force conclusion of a transaction whose progress has stalled
        member __.Push(transactionId) = async {
            let! _ = execute transactionId None in () }