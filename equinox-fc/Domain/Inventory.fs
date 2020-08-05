namespace Fc.Domain.Inventory

open Equinox.Core // we use Equinox's AsyncCacheCell helper below

type internal IdsCache<'Id>() =
    let all = System.Collections.Concurrent.ConcurrentDictionary<'Id, unit>() // Bounded only by relatively low number of physical pick tickets IRL
    static member Create init = let x = IdsCache() in x.Add init; x
    member __.Add ids = for x in ids do all.[x] <- ()
    member __.Contains id = all.ContainsKey id

/// Ingests items into a log of items, making a best effort at deduplicating as it writes
/// Prior to first add, reads recent ids, in order to minimize the number of duplicated Ids we ingest
type Service internal (inventoryId, epochs : Epoch.Service) =

    static let log = Serilog.Log.ForContext<Service>()

    // We want max one request in flight to establish the pre-existing Events from which the TransactionIds cache will be seeded
    let previousIds : AsyncCacheCell<Set<InventoryTransactionId>> =
        let read = async { let! r = epochs.TryIngest(inventoryId, Seq.empty) in return r.transactionIds }
        AsyncCacheCell read

    // TransactionIds cache - used to maintain a list of transactions that have already been ingested in order to avoid db round-trips
    let previousIds : AsyncCacheCell<IdsCache<_>> = AsyncCacheCell <| async {
        let! previousIds = previousIds.AwaitValue()
        return IdsCache.Create(previousIds) }

    let tryIngest events = async {
        let! previousIds = previousIds.AwaitValue()

        let rec aux totalIngested items = async {
            let SeqPartition f = Seq.toArray >> Array.partition f
            let dup, fresh = items |> SeqPartition (Epoch.Events.chooseInventoryTransactionId >> Option.exists previousIds.Contains)
            let fullCount = List.length items
            let dropping = fullCount - Array.length fresh
            if dropping <> 0 then log.Information("Ignoring {count}/{fullCount} duplicate ids: {ids}", dropping, fullCount, dup)
            if Array.isEmpty fresh then
                return totalIngested
            else
                let! res = epochs.TryIngest(inventoryId, fresh)
                log.Information("Added {count} items to {inventoryId:l}", res.added, inventoryId)
                // The adding is potentially redundant; we don't care
                previousIds.Add res.transactionIds
                let totalIngestedTransactions = totalIngested + res.added
                return totalIngestedTransactions }
        return! aux 0 events
    }

    /// Upon startup, we initialize the TransactionIds cache with recent epochs; we want to kick that process off before our first ingest
    member __.Initialize() = previousIds.AwaitValue() |> Async.Ignore

    /// Feeds the events into the sequence of transactions. Returns the number actually added [excluding duplicates]
    member __.Ingest(events : Epoch.Events.Event list) : Async<int> = tryIngest events

module internal Helpers =

    let create inventoryId epochs =
        Service(inventoryId, epochs)

module EventStore =

    let create inventoryId (context, cache) =
        let epochs = Epoch.EventStore.create (context, cache)
        Helpers.create inventoryId epochs
