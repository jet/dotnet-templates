module Shipping.Domain.FinalizationProcessManager

type Service
    (   transactions : FinalizationTransaction.Service,
        containers : Container.Service,
        shipments : Shipment.Service,
        /// Maximum parallelism factor when fanning out Assign or Revoke work across Shipments
        maxDop) =

    let execute (transactionId : TransactionId) : FinalizationTransaction.Events.Event option -> Async<bool> =
        let rec loop (update: FinalizationTransaction.Events.Event option) = async {
            let loop event = loop (Some event)

            let! next = transactions.Apply(transactionId, update)

            match next with
            | FinalizationTransaction.Action.AssignShipments (containerId, shipmentIds) ->
                let tryAssign sId = async {
                    let! res = shipments.TryAssign(sId, containerId)
                    return if res then None else Some sId
                }

                let! assigmentOutcomes = Async.Parallel(seq { for sId in shipmentIds -> tryAssign sId }, maxDop)

                match Array.choose id assigmentOutcomes with
                | [||] ->
                    return! loop FinalizationTransaction.Events.AssignmentCompleted
                | failedShipmentAssignments ->
                    let inDoubt = shipmentIds |> Array.except failedShipmentAssignments
                    return! loop (FinalizationTransaction.Events.RevertCommenced {| shipmentIds = inDoubt |})

              | FinalizationTransaction.Action.RevertAssignment (containerId, shipmentIds) ->
                  let! _ = Async.Parallel(seq { for sId in shipmentIds -> shipments.TryRevoke(sId, containerId) }, maxDop)
                  return! loop FinalizationTransaction.Events.Completed

              | FinalizationTransaction.Action.FinalizeContainer (containerId, shipmentIds) ->
                  do! containers.Finalize(containerId, shipmentIds)
                  return! loop FinalizationTransaction.Events.Completed

              | FinalizationTransaction.Action.Finish result ->
                  return result
        }
        loop

    // Caller should generate the TransactionId via a deterministic hash of the shipmentIds in order to ensure idempotency (and sharing of fate) of identical requests
    member __.TryFinalizeContainer(transactionId, containerId, shipmentIds) : Async<bool> =
        let initialRequest = FinalizationTransaction.Events.FinalizationRequested {| containerId = containerId; shipmentIds = shipmentIds |}
        execute transactionId (Some initialRequest)

    /// Used by watchdog service to drive processing to a conclusion where a given request was orphaned
    member __.Drive(transactionId : TransactionId) =
        execute transactionId None
