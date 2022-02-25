namespace Shipping.Watchdog.Integration

open Shipping.Watchdog
open System

/// Manages creation, log capture and correct shutdown (inc dumping stats) of a Reactor instance over a Memory Store
/// Not a Class Fixture as a) it requires TestOutput b) we want to guarantee no residual state from preceding tests (esp if they timed out)
type MemoryReactorFixture(testOutput) =
    let store = new MemoryStoreFixture()
    let statsFreq, stateFreq = TimeSpan.FromSeconds 10., TimeSpan.FromMinutes 1.
    let log = XunitLogger.forTest testOutput
    let buildProjector filter handle =
        let stats = Handler.Stats(log, statsFreq, stateFreq)
        let sink =
            let maxReadAhead, maxConcurrentStreams = Int32.MaxValue, 4
            Program.createProjector log (maxReadAhead, maxConcurrentStreams) handle stats
        let projector = MemoryStoreProjector.Start(log, sink)
        let projectorStoreSubscription =
            match store.Config with
            | Shipping.Domain.Config.Store.Memory store -> projector.Subscribe(store.Committed |> Observable.filter (fst >> filter))
            | x -> failwith $"unexpected store config %A{x}"
        projectorStoreSubscription, projector, stats
    let processingTimeout, maxDop = TimeSpan.FromSeconds 1., 4
    let engine = Shipping.Domain.FinalizationWorkflow.Config.createEngine maxDop store.Config
    let handle = Handler.createHandler processingTimeout engine
    let storeSub, projector, stats = buildProjector Handler.isRelevant handle

    member val Engine = engine
    member val RunTimeout = TimeSpan.FromSeconds 0.1
    member val Log = log

    /// Waits until all <c>Submit</c>ted batches have been fed through the <c>inner</c> Projector
    member _.AwaitWithStopOnCancellation() = projector.AwaitWithStopOnCancellation()

    interface IDisposable with

        /// Stops the projector, emitting the final stats to the log
        member _.Dispose() =
            (store :> IDisposable).Dispose()
            storeSub.Dispose()
            stats.DumpStats()
