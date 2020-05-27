module Shipping.Domain.Tests.FinalizationProcessManagerTests

open Shipping.Domain

open FsCheck.Xunit
open Swensen.Unquote

[<Property>]
let ``FinalizationProcessManager properties`` (Id transId1, Id transId2, Id containerId1, Id containerId2, IdsMoreThanOne shipmentIds1, IdsMoreThanOne shipmentIds2, Id shipment3) =
    let store = Equinox.MemoryStore.VolatileStore()
    let buffer = EventAccumulator()
    use __ = store.Committed.Subscribe buffer.Record
    let eventTypes = seq { for e in buffer.All() -> e.EventType }
    let processManager = createProcessManager 16 store
    Async.RunSynchronously <| async {
        (* First, run the happy path - should pass through all stages of the lifecycle *)
        let requestedShipmentIds = Array.append shipmentIds1 shipmentIds2
        let! res1 = processManager.TryFinalizeContainer(transId1, containerId1, requestedShipmentIds)
        let expectedEvents =
            [   "FinalizationRequested"; "ReservationCompleted"; "AssignmentCompleted"; "Completed" // Transaction
                "Reserved"; "Assigned" // Shipment
                "Finalized"] // Container
        test <@ res1 && set eventTypes = set expectedEvents @>
        let containerEvents =
            buffer.Queue(Container.streamName containerId1)
            |> Seq.choose Container.Events.codec.TryDecode
            |> List.ofSeq
        test <@ match containerEvents with
                | [ Container.Events.Finalized e ] -> e.shipmentIds = requestedShipmentIds
                | xs -> failwithf "Unexpected %A" xs @>
        (* Next, we run an overlapping finalize - this should
           a) yield a fail result
           b) result in triggering of Revert flow with associated Shipment revoke events *)
        buffer.Clear()
        let! res2 = processManager.TryFinalizeContainer(transId2, containerId2, Array.append shipmentIds2 [|shipment3|])
        test <@ not res2
                && set eventTypes = set ["FinalizationRequested"; "RevertCommenced"; "Completed"; "Reserved"; "Revoked"] @>
    }

module Dummy = let [<EntryPoint>] main _argv = 0
