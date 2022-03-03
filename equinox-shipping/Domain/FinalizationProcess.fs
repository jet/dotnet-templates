module Shipping.Domain.FinalizationProcess

open FinalizationTransaction

type internal Service internal (transactions : FinalizationTransaction.Service, containers : Container.Service, shipments : Shipment.Service) =

    member internal _.TryReserveShipment(shipmentId, transactionId) =
        shipments.TryReserve(shipmentId, transactionId)

    member internal _.AssignShipment(shipmentId, transactionId, containerId) =
        shipments.Assign(shipmentId, containerId, transactionId)

    member internal _.RevokeShipmentReservation(shipmentId, transactionId) =
        shipments.Revoke(shipmentId, transactionId)

    member internal _.FinalizeContainer(containerId, shipmentIds) =
        containers.Finalize(containerId, shipmentIds)

    member internal _.Step(transactionId, update) =
        transactions.Step(transactionId, update)

type Manager internal (service : Service, maxDop) =

    let rec run transactionId update = async {
        let loop updateEvent = run transactionId (Some updateEvent)
        match! service.Step(transactionId, update) with
        | Flow.ReserveShipments shipmentIds ->
            let tryReserve sId = async {
                let! res = service.TryReserveShipment(sId, transactionId)
                return if res then None else Some sId
            }

            let! outcomes = Async.Parallel(shipmentIds |> Seq.map tryReserve, maxDop)

            match Array.choose id outcomes with
            | [||] ->
                return! loop Events.ReservationCompleted
            | failedReservations ->
                let inDoubt = shipmentIds |> Array.except failedReservations
                return! loop (Events.RevertCommenced {| shipments = inDoubt |})

        | Flow.RevertReservations shipmentIds ->
            let! _ = Async.Parallel(seq { for sId in shipmentIds -> service.RevokeShipmentReservation(sId, transactionId) }, maxDop)
            return! loop Events.Completed

        | Flow.AssignShipments (shipmentIds, containerId) ->
            let! _ = Async.Parallel(seq { for sId in shipmentIds -> service.AssignShipment(sId, transactionId, containerId) }, maxDop)
            return! loop Events.AssignmentCompleted

        | Flow.FinalizeContainer (containerId, shipmentIds) ->
            do! service.FinalizeContainer(containerId, shipmentIds)
            return! loop Events.Completed

        | Flow.Finish result ->
            return result
        }

    /// Used by watchdog service to drive processing to a conclusion where a given request was orphaned
    member _.Pump(transactionId : TransactionId) =
        run transactionId None

    // Caller should generate the TransactionId via a deterministic hash of the shipmentIds in order to ensure idempotency (and sharing of fate) of identical requests
    member _.TryFinalizeContainer(transactionId, containerId, shipmentIds) : Async<bool> =
        if Array.isEmpty shipmentIds then invalidArg "shipmentIds" "must not be empty"
        let initialRequest = Events.FinalizationRequested {| container = containerId; shipments = shipmentIds |}
        run transactionId (Some initialRequest)

module Config =

    let private createService store =
        let transactions = Config.create store
        let containers = Container.Config.create store
        let shipments = Shipment.Config.create store
        Service(transactions, containers, shipments)
    let create maxDop store =
        Manager(createService store, maxDop = maxDop)
