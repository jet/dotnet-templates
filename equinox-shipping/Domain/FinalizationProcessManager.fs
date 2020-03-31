module FinalizationProcessManager

    open FSharp.UMX
    open Types

    type Service(transactions : FinalizationTransaction.Service, containers : Container.Service, shipments : Shipment.Service) =

        // Transaction ID can be a md5 hash of all shipments ID
        let execute (transactionId : string<transactionId>) : FinalizationTransaction.Events.Event option -> Async<bool> =
            let rec loop (update: FinalizationTransaction.Events.Event option) =
                async {
                    let loop event = loop (Some event)

                    let! action =
                        transactions.Apply(transactionId, update)

                    let untag (ids: string<shipmentId>[]) : string[] = ids |> Array.map UMX.untag

                    match action with
                    | FinalizationTransaction.Action.AssignShipments (containerId, shipmentIds) ->
                        let! result =
                            seq {
                                for sId in shipmentIds -> async {
                                    let! res = shipments.Execute(sId, Shipment.Command.Assign containerId)
                                    return (sId, res)
                                }
                            }
                            |> Async.Parallel

                        let failures =
                            Array.partition snd result
                            |> snd
                            |> Array.map fst

                        return!
                            match failures with
                            | [||]     -> loop (FinalizationTransaction.Events.AssignmentCompleted {| shipmentIds = untag shipmentIds |})
                            | failures -> loop (FinalizationTransaction.Events.RevertRequested {| shipmentIds = untag failures |})

                     | FinalizationTransaction.Action.FinalizeContainer (containerId, shipmentIds) ->
                         do! containers.Execute (containerId, Container.Command.Finalize shipmentIds)
                         return! loop FinalizationTransaction.Events.Completed

                     | FinalizationTransaction.Action.RevertAssignment shipmentIds ->
                         let! _ = Async.Parallel(seq { for sId in shipmentIds -> shipments.Execute(sId, Shipment.Command.Unassign)})
                         return! loop FinalizationTransaction.Events.Completed

                     | FinalizationTransaction.Action.Finish result ->
                         return result
                }
            loop

        member __.TryFinalize(transactionId : string<transactionId>, containerId : string, shipmentIds : string[]) =
            execute transactionId (Some <| FinalizationTransaction.Events.FinalizationRequested {| containerId = containerId; shipmentIds = shipmentIds |})

        member __.Drive(transactionId : string<transactionId>) =
            execute transactionId None