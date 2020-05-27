module Shipping.Watchdog.Integration.WatchdogIntegrationTests

open System.Collections.Generic
open Serilog
open Shipping.Domain
open Shipping.Watchdog

open FsCheck.Xunit
open Swensen.Unquote
open System

open System.Collections.Concurrent

type EventAccumulator<'S, 'E>() =
    let messages = ConcurrentDictionary<'S,ConcurrentQueue<'E>>()

    member __.Record(stream : 'S, events : 'E seq) =
        let initStreamQueue _ = ConcurrentQueue events
        let appendToQueue _ (queue : ConcurrentQueue<'E>) = events |> Seq.iter queue.Enqueue; queue
        messages.AddOrUpdate(stream, initStreamQueue, appendToQueue) |> ignore

    member __.Queue stream =
        match messages.TryGetValue stream with
        | false, _ -> Seq.empty<'E>
        | true, xs -> xs :> _

    member __.All() = seq { for KeyValue (_, xs) in messages do yield! xs }

    member __.Clear() =
        messages.Clear()

module FinalizationTransaction =
    open FinalizationTransaction
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let resolver = Resolver(store, Events.codec, Fold.fold, Fold.initial)
            create resolver.Resolve
module Container =
    open Container
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let resolver = Resolver(store, Events.codec, Fold.fold, Fold.initial)
            create resolver.Resolve
module Shipment =
    open Shipment
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let resolver = Resolver(store, Events.codec, Fold.fold, Fold.initial)
            create resolver.Resolve

let createProcessManager maxDop store =
    let transactions = FinalizationTransaction.MemoryStore.create store
    let containers = Container.MemoryStore.create store
    let shipments = Shipment.MemoryStore.create store
    FinalizationProcessManager.Service(transactions, containers, shipments, maxDop=maxDop)

open System.Threading

type MemoryStoreSource<'F>(sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<Propulsion.Streams.StreamEvent<'F> seq, _>>) =
    let ingester = sink.StartIngester(Serilog.Log.Logger, 0)
    let mutable epoch = -1L
    let mutable completed = None
    let mutable checkpointed = None

    member __.Submit(stream, events : 'E seq) =
        let epoch = Interlocked.Increment &epoch
        let markCompleted () = completed <- Some epoch
        let checkpoint = async { checkpointed <- Some epoch }
        ingester.Submit(epoch, checkpoint, seq { for x in events -> { stream = stream; event = x } }, markCompleted)
        |> Async.Ignore
        |> Async.RunSynchronously
        // TODO use a ProducerConsumerCollection of some kind

    member __.AwaitCompletion(?delay, ?logInterval, ?log) = async {
        let delay = defaultArg delay TimeSpan.FromMilliseconds 5.
        let maybeLog =
            let logInterval = defaultArg logInterval (TimeSpan.FromMinutes 1.)
            let logDue = Propulsion.Internal.intervalCheck logInterval
            let log = defaultArg log Serilog.Log.Logger
            fun () ->
                if logDue () then
                    log.Information("Waiting for epoch {epoch} (completed to {completed}, checkpointed to {checkpoint})",
                        epoch, completed, checkpointed)
        let delayMs = int delay.TotalMilliseconds
        while Some (Volatile.Read &epoch) <> Volatile.Read &completed do
            maybeLog()
            do! Async.Sleep delayMs
        sink.Stop()
        return! sink.AwaitCompletion()
    }

type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer);
        writer |> string |> testOutput.WriteLine
        writer |> string |> System.Diagnostics.Debug.Write
    interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

let (|Id|) (x : System.Guid) = x.ToString "N" |> FSharp.UMX.UMX.tag
let (|Ids|) (xs : System.Guid[]) = xs |> Array.map (|Id|)
let (|AtLeastOne|) (x, xs) = x::xs

type Async with

    /// Returns an async computation which runs the argument computation but raises an exception if it doesn't complete
    /// by the specified timeout.
    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) = async {
        let! r = Async.StartChild(c, (int)timeout.TotalMilliseconds)
        return! r }

type Tests(output) =

    let logger = TestOutputAdapter output
    let log = LoggerConfiguration().Destructure.FSharpTypes().WriteTo.Sink(logger).CreateLogger()

    [<Property(StartSize=1000, MaxTest=5, MaxFail=1)>]
    let ``Watchdog.Handler properties`` (AtLeastOne batches) =
        let store = Equinox.MemoryStore.VolatileStore()
        let processManager = createProcessManager 4 store

        let runTimeout, processingTimeout = TimeSpan.FromSeconds 0.1, TimeSpan.FromSeconds 1.
        let maxReadAhead, maxConcurrentStreams = Int32.MaxValue, 4

        let stats = Handler.Stats(log, TimeSpan.FromSeconds 10., TimeSpan.FromMinutes 1.)
        let watchdogSink = Program.createSink log (processingTimeout, stats) (maxReadAhead, maxConcurrentStreams) processManager.Drive
        Async.RunSynchronously <| async {
            let source = MemoryStoreSource(watchdogSink)
            use __ =
                store.Committed
                |> Observable.filter (fun (s,_e) -> Handler.isRelevant s)
                |> Observable.subscribe source.Submit

            let counts = Stack()
            let mutable timeouts = 0
            for (Id tid, Id cid, Id oneSid, Ids otherSids) in batches do
                let shipmentIds = [| oneSid; yield! otherSids |]
                counts.Push shipmentIds.Length
                try let! _ = processManager.TryFinalizeContainer(tid, cid, shipmentIds)
                             |> Async.timeoutAfter runTimeout
                    ()
                with :? TimeoutException -> timeouts <- timeouts + 1

            log.Information("Awaiting batches: {counts} ({timeouts}/{total} timeouts)", counts, timeouts, counts.Count)
            do! source.AwaitCompletion(logInterval=TimeSpan.FromSeconds 0.5, log=log)
            stats.DumpStats()
        }

module Dummy = let [<EntryPoint>] main _argv = 0