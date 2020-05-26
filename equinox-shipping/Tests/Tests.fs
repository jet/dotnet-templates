module Shipping.Tests

open System.Text
open FsCheck.Xunit
open Swensen.Unquote
open System.Collections.Concurrent
open Xunit.Abstractions

module FinalizationTransaction =
    open Domain.FinalizationTransaction
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let resolver = Resolver(store, Events.codec, Fold.fold, Fold.initial)
            create resolver.Resolve
module Container =
    open Domain.Container
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let resolver = Resolver(store, Events.codec, Fold.fold, Fold.initial)
            create resolver.Resolve
module Shipment =
    open Domain.Shipment
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let resolver = Resolver(store, Events.codec, Fold.fold, Fold.initial)
            create resolver.Resolve

let createProcessManager maxDop store =
    let transactions = FinalizationTransaction.MemoryStore.create store
    let containers = Container.MemoryStore.create store
    let shipments = Shipment.MemoryStore.create store
    Domain.FinalizationProcessManager.Service(transactions, containers, shipments, maxDop=maxDop)

(* Generic FsCheck helpers *)
open System
let (|Id|) (x : Guid) = x.ToString "N" |> FSharp.UMX.UMX.tag
let inline mkId () = Guid.NewGuid() |> (|Id|)
let (|Ids|) (xs : Guid[]) = xs |> Array.map (|Id|)
//let (|ToList|) (xs : 'T[]) = Array.toList xs
let (|IdsMoreThanOne|) (Ids xs, Id x) = [| yield x; yield! xs |]

type ChangeBuffer<'S, 'E>() =
    let messages = ConcurrentDictionary<'S,ConcurrentQueue<'E>>()

    member __.Record(stream : 'S, events : 'E seq) =
        let initStreamQueue _ = ConcurrentQueue events
        let appendToQueue _ (queue : ConcurrentQueue<'E>) = events |> Seq.iter queue.Enqueue; queue
        let _ = messages.AddOrUpdate(stream, initStreamQueue, appendToQueue)
        ()

    member __.Queue stream =
        match messages.TryGetValue stream with
        | false, _ -> Seq.empty<'E>
        | true, xs -> xs :> _

    member __.All() = seq {
        for KeyValue (_, xs) in messages do
            for y in xs do
                yield y
    }

    member __.Clear() =
        messages.Clear()

type Tests(output : ITestOutputHelper) =
    //let [<Xunit.Fact>] x () = 1 =! 0
    [<Property>]
    let ``Finalize MemoryStore properties`` (Id transId, Id containerId, IdsMoreThanOne shipmentIds) =
        let store = Equinox.MemoryStore.VolatileStore()
        let buffer = ChangeBuffer()
        use __ = store.Committed.Subscribe buffer.Record
        let pm = createProcessManager 16 store
        Async.RunSynchronously <| async {
            let! res = pm.TryFinalizeContainer(transId, containerId, shipmentIds)
            test <@ res @>
        }

    [<Property>]
    let ``Conflict rolls back all other reservations`` (Id transId1, Id transId2, Id containerId1, Id containerId2, IdsMoreThanOne shipmentIds1, IdsMoreThanOne shipmentIds2) =
        let store = Equinox.MemoryStore.VolatileStore()
        let buffer = ChangeBuffer()
        use __ = store.Committed.Subscribe buffer.Record
        let pm = createProcessManager 16 store
        Async.RunSynchronously <| async {
            let! res1 = pm.TryFinalizeContainer(transId1, containerId1, Array.append shipmentIds1 shipmentIds2)
            buffer.Clear()
            let! res2 = pm.TryFinalizeContainer(transId2, containerId2, shipmentIds2)
            test <@ ((not << Array.isEmpty) shipmentIds2 && res1 && not res2)
                    || buffer.All() |> Seq.map (fun x -> x.EventType) |> Seq.distinct |> Seq.isEmpty @>
        }

module Dummy = let [<EntryPoint>] main _argv = 0