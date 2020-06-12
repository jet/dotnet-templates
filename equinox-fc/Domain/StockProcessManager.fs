module Fc.Domain.StockProcessManager

type Service(transactions : StockTransaction.Service, locations : Location.Service, inventory : Inventory.Service) =

    let execute transactionId =
        let f = Location.Epoch.decide transactionId
        let rec aux update = async {
            let! action = transactions.Apply(transactionId, update)
            let aux event = aux (Some event)
            match action with
            | StockTransaction.Adjust (loc, bal) ->
                match! locations.Execute(loc, f (Location.Epoch.Reset bal)) with
                | Location.Epoch.Accepted _ -> return! aux StockTransaction.Events.Adjusted
                | Location.Epoch.Denied -> return failwith "Cannot Deny Reset"
                | Location.Epoch.DupFromPreviousEpoch -> return failwith "TODO walk back to previous epoch"
            | StockTransaction.Remove (loc, delta) ->
                match! locations.Execute(loc, f (Location.Epoch.Remove delta)) with
                | Location.Epoch.Accepted bal -> return! aux (StockTransaction.Events.Removed { balance = bal })
                | Location.Epoch.Denied -> return! aux StockTransaction.Events.Failed
                | Location.Epoch.DupFromPreviousEpoch -> return failwith "TODO walk back to previous epoch"
            | StockTransaction.Add    (loc, delta) ->
                match! locations.Execute(loc, f (Location.Epoch.Add delta)) with
                | Location.Epoch.Accepted bal -> return! aux (StockTransaction.Events.Added   { balance = bal })
                | Location.Epoch.Denied -> return failwith "Cannot Deny Add"
                | Location.Epoch.DupFromPreviousEpoch -> return failwith "TODO walk back to previous epoch"
            | StockTransaction.Log (StockTransaction.Adjusted _) ->
                let! _count = inventory.Ingest([Inventory.Epoch.Events.Adjusted    { transactionId = transactionId }])
                return! aux StockTransaction.Events.Logged
            | StockTransaction.Log (StockTransaction.Transferred _) ->
                let! _count = inventory.Ingest([Inventory.Epoch.Events.Transferred { transactionId = transactionId }])
                return! aux StockTransaction.Events.Logged
            | StockTransaction.Finish success ->
                return success
        }
        aux
    let run transactionId req = execute transactionId (Some req)

    member __.Adjust(transactionId, location, quantity) =
        run transactionId (StockTransaction.Events.AdjustmentRequested { location = location; quantity = quantity })

    member __.TryTransfer(transactionId, source, destination, quantity) =
        run transactionId (StockTransaction.Events.TransferRequested { source = source; destination = destination; quantity = quantity })

    /// Used by Watchdog to force conclusion of a transaction whose progress has stalled
    member __.Drive(transactionId) = async {
        let! _ = execute transactionId None in () }

module EventStore =

    let create inventoryId (epochLen, idsWindow, maxAttempts) (context, cache) =
        let transactions, locations, inventory =
            let transactions = StockTransaction.EventStore.create (context, cache)
            let zero, cf, sc = (Location.Epoch.zeroBalance, Location.Epoch.toBalanceCarriedForward idsWindow, Location.Epoch.shouldClose epochLen)
            let locations = Location.EventStore.create (zero, cf, sc) (context, cache, maxAttempts)
            let inventory = Inventory.EventStore.create inventoryId (context, cache)
            transactions, locations, inventory
        Service(transactions, locations, inventory)
