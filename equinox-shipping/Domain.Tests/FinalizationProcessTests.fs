module Shipping.Domain.Tests.FinalizationProcessTests

open Shipping.Domain

open FsCheck
open FsCheck.FSharp
open FsCheck.Xunit
open FSharp.UMX
open Propulsion.Internal // Seq.chooseV
open Swensen.Unquote

type GuidStringN<[<Measure>]'m> = GuidStringN of string<'m> with static member op_Explicit(GuidStringN x) = x
let (|Ids|) = Array.map (function GuidStringN x -> x)

let genDefault<'t> = ArbMap.defaults |> ArbMap.generate<'t>
type Custom =

    static member GuidStringN() = genDefault |> Gen.map (Guid.toStringN >> GuidStringN) |> Arb.fromGen

[<assembly: Properties( Arbitrary = [| typeof<Custom> |] )>] do()

module FE = FinalizationTransaction.Events

type Properties(testOutput) =

    let store = new MemoryStoreFixture(TestOutput = testOutput) // Run under debugger and/or adjust XunitLogger.minLevel to see events in test output

    let [<Property>] finalization
        (   GuidStringN transId1, GuidStringN transId2, GuidStringN containerId1, GuidStringN containerId2,
            NonEmptyArray (Ids shipmentIds1), NonEmptyArray (Ids shipmentIds2), GuidStringN shipment3) = async {
        let buffer = EventAccumulator()
        use _ = store.Committed.Subscribe buffer.Record
        let eventTypes = seq { for e in buffer.All() -> e.EventType }
        let manager = FinalizationProcess.Factory.create 16 store.Config

        (* First, run the happy path - should pass through all stages of the lifecycle *)
        let requestedShipmentIds = Array.append shipmentIds1 shipmentIds2
        let! res1 = manager.TryFinalizeContainer(transId1, containerId1, requestedShipmentIds)
        let expectedEvents =
            [   nameof(FE.FinalizationRequested); nameof(FE.ReservationCompleted); nameof(FE.AssignmentCompleted); nameof(FE.Completed) // Transaction
                nameof(Shipment.Events.Reserved); nameof(FE.Assigned) // Shipment
                nameof(Container.Events.Finalized)] // Container
        test <@ res1 && set eventTypes = set expectedEvents @>
        let containerEvents =
            buffer.Queue(Container.Reactions.streamName containerId1)
            |> Seq.chooseV (FsCodec.Encoder.Uncompressed Container.Events.codec).Decode 
            |> List.ofSeq
        test <@ match containerEvents with
                | [ Container.Events.Finalized e ] -> e.shipmentIds = requestedShipmentIds
                | xs -> xs |> failwithf "Unexpected %A" @>
        (* Next, we run an overlapping finalize - this should
           a) yield a fail result
           b) result in triggering of Revert flow with associated Shipment revoke events *)
        buffer.Clear()
        let! res2 = manager.TryFinalizeContainer(transId2, containerId2, Array.append shipmentIds2 [|shipment3|])
        let expectedEvents =
            [   nameof(FE.FinalizationRequested); nameof(FE.RevertCommenced); nameof(FE.Completed) // Transaction
                nameof(Shipment.Events.Reserved); nameof(Shipment.Events.Revoked) ] // Shipment
        test <@ not res2
                && set eventTypes = set expectedEvents @>
        testOutput.WriteLine "Iteration completed" }
