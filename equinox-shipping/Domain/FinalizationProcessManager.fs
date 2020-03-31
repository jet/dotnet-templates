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
                        // TODO: This needs throttling on Cosmos.
                        let! result =
                            seq {
                                for sId in shipmentIds -> async {
                                    let! res = shipments.Execute(sId, Shipment.Command.Assign containerId)
                                    return (sId, res)
                                }
                            }
                            |> Async.Parallel

                        let (successes, failures) =
                            Array.partition snd result

                        return!
                            match Array.map fst failures with
                            | [||] -> loop (FinalizationTransaction.Events.AssignmentCompleted {| shipmentIds = untag shipmentIds |})
                            | _    -> loop (FinalizationTransaction.Events.RevertRequested     {| shipmentIds = successes |> Array.map (fst >> UMX.untag) |})

                     | FinalizationTransaction.Action.FinalizeContainer containerId ->
                         do! containers.Execute (containerId, Container.Command.Finalize)
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