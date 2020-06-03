namespace Shipping.Watchdog.Integration

open Shipping.Domain.Tests
open Shipping.Watchdog

open FsCheck.Xunit
open System

type WatchdogIntegrationTests(output) =

    let log = TestOutputLogger.create output

    [<Property(StartSize=1000, MaxTest=5, MaxFail=1)>]
    let ``Watchdog.Handler properties`` (AtLeastOne batches) =
        let store = Equinox.MemoryStore.VolatileStore()
        let processManager = Shipping.Domain.Tests.Fixtures.createProcessManager 4 store

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

            let counts = System.Collections.Generic.Stack()
            let mutable timeouts = 0
            for (Id tid, Id cid, IdsMoreThanOne shipmentIds) in batches do
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