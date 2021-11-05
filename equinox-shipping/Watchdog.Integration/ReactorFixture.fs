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
        | Config.Store.Memory store -> projector.Subscribe(store.Committed |> Observable.filter (fun (s,_e) -> Handler.isRelevant s))
        | x -> failwith $"unexpected store config %A{x}"

    member val ProcessManager = processManager
    member val RunTimeout = TimeSpan.FromSeconds 0.1
    member val Log = log

    /// Waits until all <c>Submit</c>ted batches have been fed into the <c>inner</c> Projector
    member _.AwaitWithStopOnCancellation() = projector.AwaitWithStopOnCancellation()

    /// Stops the projector, emitting the final stats to the log
    interface IDisposable with
        member _.Dispose() =
            (store :> IDisposable).Dispose()
            projectorStoreSubscription.Dispose()
            stats.DumpStats()

/// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.CosmosStore CFP
/// See SerilogLogFixture for details of how to expose complete diagnostic messages
type CosmosReactorFixture(messageSink) =
    let serilogLog = new SerilogLogFixture(messageSink) // create directly to ensure correct sequencing and no loss of messages
    let cosmos = CosmosStoreConnector()
    let storeCfg, monitored = cosmos.Connect()
    let leases = cosmos.ConnectLeases()
    let statsFreq, stateFreq = TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 2.
    let stats = Handler.Stats(Serilog.Log.Logger, statsFreq, stateFreq)
    let processManagerDop = 4
    let processManager = Shipping.Domain.FinalizationWorkflow.Config.create processManagerDop storeCfg
    let sink =
        let processingTimeout = TimeSpan.FromSeconds 1.
        let maxReadAhead, maxConcurrentStreams = 1024, 4 // TODO make Int32.MaxValue work
        Program.startWatchdog Serilog.Log.Logger (processingTimeout, stats) (maxReadAhead, maxConcurrentStreams) processManager.Pump
    let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Serilog.Log.Logger, sink.StartIngester, Seq.collect Handler.transformOrFilter)
    let withStartTime1sAgo (x : Microsoft.Azure.Cosmos.ChangeFeedProcessorBuilder) = x.WithStartTime(let now = DateTime.UtcNow in now.AddSeconds -1.)
    let consumerGroupName = "ReactorFixture:" + (let g = Guid.NewGuid() in g.ToString "N")
    let pipeline =
        Propulsion.CosmosStore.CosmosStoreSource.Run(
            Serilog.Log.Logger, monitored, leases, consumerGroupName, observer,
            lagReportFreq = TimeSpan.FromMinutes 1., startFromTail = false, customize = withStartTime1sAgo)
    do Async.Start pipeline

    member val Log = Serilog.Log.Logger
    member val ProcessManager = processManager
    member val RunTimeout = TimeSpan.FromSeconds 2.

    /// As this is a Collection Fixture, it will outlive an individual instantiation of a Test Class
    /// This enables us to tee the output that normally goes to the Test Runner Diagnostic Sink to the test output of the (by definition, single) current test
    member _.CaptureSerilogLog(testOutput) = serilogLog.CaptureSerilogLog testOutput
    member _.DumpStats() = stats.DumpStats()

    /// Stops the projector, emitting the final stats to the log
    interface IDisposable with
        member _.Dispose() =
            // NB Disposable TODO fix disposal in dotnet-templates / propulsion
            observer.Dispose()
            sink.Stop()
            stats.DumpStats()
            (serilogLog :> IDisposable).Dispose()

[<Xunit.CollectionDefinition "CosmosReactor">]
type CosmosReactorCollection() =
    interface Xunit.ICollectionFixture<CosmosReactorFixture>
