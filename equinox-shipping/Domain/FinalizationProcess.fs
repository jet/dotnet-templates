module Shipping.Domain.FinalizationProcess

open FinalizationTransaction

type internal Service internal (transactions: FinalizationTransaction.Service, containers: Container.Service, shipments: Shipment.Service) =

    member internal _.TryReserveShipment(shipmentId, transactionId) =
        shipments.TryReserve(shipmentId, transactionId)

    member internal _.AssignShipment(shipmentId, transactionId, containerId) =
        shipments.Assign(shipmentId, containerId, transactionId)

    member internal _.RevokeShipmentReservation(shipmentId, transactionId) =
        shipments.Revoke(shipmentId, transactionId)

    member internal _.FinalizeContainer(containerId, shipmentIds) =
        containers.Finalize(containerId, shipmentIds)

    member internal _.Read transactionId = transactions.Read transactionId
    member internal _.Start(transactionId, containerId, shipments) = transactions.Start(transactionId, containerId, shipments) 
    member internal _.RecordCompleted transactionId = transactions.RecordCompleted transactionId
    member internal _.RecordAssignmentCompleted transactionId = transactions.RecordAssignmentCompleted transactionId
    member internal _.RecordReservationCompleted transactionId = transactions.RecordReservationCompleted transactionId
    member internal _.StartRevert(transactionId, failedReservations) = transactions.StartRevert(transactionId, failedReservations) 

type Manager internal (service: Service, maxDop) =

    let handle transactionId phase = async {
        match phase with
        | Flow.ReserveShipments shipmentIds ->
            let! outcomes = Async.parallelLimit maxDop [| for sId in shipmentIds -> service.TryReserveShipment(sId, transactionId) |]
            match [| for sId, reserved in Seq.zip shipmentIds outcomes do if not reserved then sId |] with
            | [||] ->
                return! service.RecordReservationCompleted transactionId
            | failedReservations ->
                return! service.StartRevert(transactionId, failedReservations)

        | Flow.RevertReservations shipmentIds ->
            do! Async.parallelDoLimit maxDop [| for sId in shipmentIds -> service.RevokeShipmentReservation(sId, transactionId) |] 
            return! service.RecordCompleted transactionId

        | Flow.AssignShipments (shipmentIds, containerId) ->
            do! Async.parallelDoLimit maxDop [| for sId in shipmentIds -> service.AssignShipment(sId, transactionId, containerId) |]
            return! service.RecordAssignmentCompleted transactionId

        | Flow.FinalizeContainer (containerId, shipmentIds) ->
            do! service.FinalizeContainer(containerId, shipmentIds)
            return! service.RecordCompleted transactionId

        | Flow.Finish _ as s -> return s }

    let run transactionId init = async {
        let rec aux phase = async {
            match phase with
            | Flow.Finish res -> return res
            | phase ->
                let! next = handle transactionId phase
                return! aux next }
        let! phase = init transactionId
        return! aux phase }

    /// Used by watchdog service to drive processing to a conclusion where a given request was orphaned
    member _.Pump transactionId =
         run transactionId service.Read

    // Caller should generate the TransactionId via a deterministic hash of the shipmentIds in order to ensure idempotency (and sharing of fate) of identical requests
    member _.TryFinalizeContainer(transactionId, containerId, shipmentIds): Async<bool> =
        if Array.isEmpty shipmentIds then invalidArg "shipmentIds" "must not be empty"
        run transactionId (fun id -> service.Start(id, containerId, shipmentIds))

module Factory =

    let private createService store =
        let transactions = Factory.create store
        let containers = Container.Factory.create store
        let shipments = Shipment.Factory.create store
        Service(transactions, containers, shipments)
    let create maxDop store =
        Manager(createService store, maxDop = maxDop)
