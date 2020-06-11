module Shipping.Domain.FinalizationProcessManager

open FinalizationTransaction

type Service
    (   transactions : FinalizationTransaction.Service,
        containers : Container.Service,
        shipments : Shipment.Service,
        /// Maximum parallelism factor when fanning out Reserve / Revoke / Assign work across Shipments
        maxDop) =

    let execute (transactionId : TransactionId) : Events.Event option -> Async<bool> =
        let rec loop (update: Events.Event option) = async {
            let loop event = loop (Some event)

            let! next = transactions.Record(transactionId, update)

            match next with
            | Action.ReserveShipments shipmentIds ->
                let tryReserve sId = async {
                    let! res = shipments.TryReserve(sId, transactionId)
                    return if res then None else Some sId
                }

                let! outcomes = Async.Parallel(shipmentIds |> Seq.map tryReserve, maxDop)

                match Array.choose id outcomes with
                | [||] ->
                    return! loop Events.ReservationCompleted
                | failedReservations ->
                    let inDoubt = shipmentIds |> Array.except failedReservations
                    return! loop (Events.RevertCommenced {| shipments = inDoubt |})

            | Action.RevertReservations shipmentIds ->
                let! _ = Async.Parallel(seq { for sId in shipmentIds -> shipments.Revoke(sId, transactionId) }, maxDop)
                return! loop Events.Completed

            | Action.AssignShipments (shipmentIds, containerId) ->
                let! _ = Async.Parallel(seq { for sId in shipmentIds -> shipments.Assign(sId, containerId, transactionId) }, maxDop)
                return! loop Events.AssignmentCompleted

            | Action.FinalizeContainer (containerId, shipmentIds) ->
                do! containers.Finalize(containerId, shipmentIds)
                return! loop Events.Completed

            | Action.Finish result ->
                return result
        }
        loop

    // Caller should generate the TransactionId via a deterministic hash of the shipmentIds in order to ensure idempotency (and sharing of fate) of identical requests
    member __.TryFinalizeContainer(transactionId, containerId, shipmentIds) : Async<bool> =
        if Array.isEmpty shipmentIds then invalidArg "shipmentIds" "must not be empty"
        let initialRequest = Events.FinalizationRequested {| container = containerId; shipments = shipmentIds |}
        execute transactionId (Some initialRequest)

    /// Used by watchdog service to drive processing to a conclusion where a given request was orphaned
    member __.Drive(transactionId : TransactionId) =
        execute transactionId None
