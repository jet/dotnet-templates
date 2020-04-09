module Shipping.Domain.FinalizationProcessManager

type Service(transactions : FinalizationTransaction.Service, containers : Container.Service, shipments : Shipment.Service, maxDop) =

    let execute (transactionId : TransactionId) : FinalizationTransaction.Events.Event option -> Async<bool> =
        let rec loop (update: FinalizationTransaction.Events.Event option) = async {
            let loop event = loop (Some event)

            let! action = transactions.Apply(transactionId, update)

            match action with
            | FinalizationTransaction.Action.AssignShipments (containerId, shipmentIds) ->
                let tryAssign sId = async {
                    let! res = shipments.TryAssign(sId, containerId)
                    return if res then None else Some sId
                }
                let work = shipmentIds |> Seq.map tryAssign
                let! failedAssignments = Async.Parallel(work, maxDop)

                let failures = Array.choose id failedAssignments
                if Array.isEmpty failures then
                    return! loop FinalizationTransaction.Events.AssignmentCompleted
                else
                    let inDoubt = shipmentIds |> Array.except failures
                    return! loop (FinalizationTransaction.Events.RevertCommenced {| shipmentIds = inDoubt |})

              | FinalizationTransaction.Action.FinalizeContainer containerId ->
                  do! containers.Finalize(containerId)
                  return! loop FinalizationTransaction.Events.Completed

              | FinalizationTransaction.Action.RevertAssignment (containerId, shipmentIds) ->
                  let! _ = Async.Parallel(seq { for sId in shipmentIds -> shipments.TryRevoke(sId, containerId) }, maxDop)
                  return! loop FinalizationTransaction.Events.Completed

              | FinalizationTransaction.Action.Finish result ->
                  return result
        }
        loop

    // Transaction ID can be a md5 hash of all shipments ID
    member __.TryFinalize(transactionId : TransactionId, containerId, shipmentIds) =
        let initialReq = FinalizationTransaction.Events.FinalizationRequested {| containerId = containerId; shipmentIds = shipmentIds |}
        execute transactionId (Some initialReq)

    member __.Drive(transactionId : TransactionId) =
        execute transactionId None
