module FinalizationTransactionProcessor

    type Service(transactions: FinalizationTransaction.Service, containers: Container.Service, shipments: Shipment.Service) =

        // Transaction ID can be a md5 hash of all shipments ID
        let execute (transactionId: string) : FinalizationTransaction.Events.Event option -> Async<bool> =
            let rec loop (update: FinalizationTransaction.Events.Event option) =
                async {
                    let loop event = loop (Some event)

                    let! action =
                        transactions.Apply(transactionId, update)

                    match action with
                    | FinalizationTransaction.Action.AssignShipments (containerId, shipmentIds) ->
                        let! result =
                            shipmentIds
                            |> Array.map (fun sId ->
                                async {
                                    let! res = shipments.Execute(sId, Shipment.Command.Assign containerId)
                                    return (sId, res)
                                }
                            )
                            |> Async.Parallel

                        let failures =
                            Array.partition snd result
                            |> snd
                            |> Array.map fst

                        return!
                            if not <| Array.isEmpty failures then
                                loop (FinalizationTransaction.Events.RevertRequested failures)
                            else
                                loop (FinalizationTransaction.Events.AssignmentCompleted (containerId, shipmentIds))

                    | FinalizationTransaction.Action.FinalizeContainer (containerId, shipmentIds) ->
                        do! containers.Execute (containerId, Container.Command.Finalize shipmentIds)

                        return! loop FinalizationTransaction.Events.FinalizationCompleted

                    | FinalizationTransaction.Action.RevertAssignment shipmentIds ->
                        do! shipmentIds
                            |> Array.map (fun sId -> shipments.Execute(sId, Shipment.Command.Unassign))
                            |> Async.Parallel
                            |> Async.Ignore

                        return! loop FinalizationTransaction.Events.FinalizationFailed

                    | FinalizationTransaction.Action.Finish result ->
                        return result
                }
            loop

        member __.TryFinalize (transactionId: string, containerId: string, shipmentIds: string[]) =
            execute transactionId (Some <| FinalizationTransaction.Events.FinalizationRequested (containerId, shipmentIds))

        member __.Push (transactionId: string) =
            execute transactionId None