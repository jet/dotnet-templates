namespace Shipping.Watchdog.Integration

open Shipping.Watchdog
open System

/// Manages creation, log capture and correct shutdown (inc dumping stats) of a Reactor instance over a Memory Store
/// Not a Class Fixture as a) it requires TestOutput b) we want to guarantee no residual state from preceding tests (esp if they timed out)
type MemoryReactorFixture(testOutput) =
    let store = new MemoryStoreFixture()
    let statsFreq, stateFreq = TimeSpan.FromSeconds 10., TimeSpan.FromMinutes 1.
    let log = XunitLogger.forTest testOutput
    let stats = Handler.Stats(log, statsFreq, stateFreq)
    let processManager = Shipping.Domain.FinalizationWorkflow.Config.create 4 store.Config
    let sink =
        let processingTimeout = TimeSpan.FromSeconds 1.
        let maxReadAhead, maxConcurrentStreams = 1024, 4 // TODO make Int32.MaxValue work
        Program.startWatchdog log (processingTimeout, stats) (maxReadAhead, maxConcurrentStreams) processManager.Pump
    let projector = MemoryStoreProjector.Start(log, sink)
    let projectorStoreSubscription =
        match store.Config with
        | Shipping.Domain.Config.Store.Memory store -> projector.Subscribe(store.Committed |> Observable.filter (fun (s,_e) -> Handler.isRelevant s))
        | x -> failwith $"unexpected store config %A{x}"

    member val ProcessManager = processManager
    member val RunTimeout = TimeSpan.FromSeconds 0.1
    member val Log = log

    /// Waits until all <c>Submit</c>ted batches have been fed into the <c>inner</c> Projector
    member _.AwaitWithStopOnCancellation() = projector.AwaitWithStopOnCancellation()

    interface IDisposable with

        /// Stops the projector, emitting the final stats to the log
        member _.Dispose() =
            (store :> IDisposable).Dispose()
            projectorStoreSubscription.Dispose()
            stats.DumpStats()
