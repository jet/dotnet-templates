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
            let maxReadAhead, maxConcurrentStreams = 4096, 4
            Program.createProjector log (maxReadAhead, maxConcurrentStreams) handle stats
        let projector = MemoryStoreProjector.Start(log, sink)
        let projectorStoreSubscription =
            match store.Config with
            | Shipping.Domain.Config.Store.Memory store -> projector.Subscribe(store.Committed |> Observable.filter (fst >> filter))
            | x -> failwith $"unexpected store config %A{x}"
        projectorStoreSubscription, projector, stats
    let processingTimeout, maxDop = TimeSpan.FromSeconds 1., 4
    let manager = Shipping.Domain.FinalizationProcess.Config.create maxDop store.Config
    let handle = Handler.createHandler processingTimeout manager
    let storeSub, projector, stats = buildProjector Handler.isRelevant handle

    member val ProcessManager = manager
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

/// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.CosmosStore CFP
/// See SerilogLogFixture for details of how to expose complete diagnostic messages
type CosmosReactorFixture(messageSink) =
    let serilogLog = new SerilogLogFixture(messageSink) // create directly to ensure correct sequencing and no loss of messages
    let cosmos = CosmosStoreConnector()
    let store, monitored = cosmos.Connect()
    let leases = cosmos.ConnectLeases()
    let buildProjector consumerGroupName filter (handle : _ -> Async<Propulsion.Streams.SpanResult * _>) =
        let statsFreq, stateFreq = TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 2.
        let stats = Handler.Stats(Serilog.Log.Logger, statsFreq, stateFreq)
        let sink =
            let maxReadAhead, maxConcurrentStreams = 1024, 4
            Propulsion.Streams.StreamsProjector.Start(Serilog.Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, statsFreq)
        let source =
            let parseFeedDoc : _ -> Propulsion.Streams.StreamEvent<_> seq = Seq.collect Handler.transformOrFilter >> Seq.filter (fun x -> filter x.stream)
            let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Serilog.Log.Logger, sink.StartIngester, parseFeedDoc)
            let withStartTime1sAgo (x : Microsoft.Azure.Cosmos.ChangeFeedProcessorBuilder) = x.WithStartTime(let t = DateTime.UtcNow in t.AddSeconds -1.)
            Propulsion.CosmosStore.CosmosStoreSource.Start(
                Serilog.Log.Logger, monitored, leases, consumerGroupName, observer,
                lagReportFreq = TimeSpan.FromMinutes 1., startFromTail = false, customize = withStartTime1sAgo)
        source, sink, stats
    let consumerGroupName = "ReactorFixture:" + (Guid.NewGuid () |> Shipping.Domain.Guid.toStringN)
    let processingTimeout, maxDop = TimeSpan.FromSeconds 1., 4
    let manager = Shipping.Domain.FinalizationProcess.Config.create maxDop store
    let handle = Handler.createHandler processingTimeout manager
    let source, sink, stats = buildProjector consumerGroupName Handler.isRelevant handle

    member val Store = store

    member val ProcessManager = manager
    member val RunTimeout = TimeSpan.FromSeconds 1.
    member val Log = Serilog.Log.Logger // initialized by CaptureSerilogLog
    /// Waits until all <c>Submit</c>ted batches have been fed through the <c>inner</c> Projector
//    member _.AwaitWithStopOnCancellation() = sink.AwaitWithStopOnCancellation()

    /// As this is a Collection Fixture, it will outlive an individual instantiation of a Test Class
    /// This enables us to tee the output that normally goes to the Test Runner Diagnostic Sink to the test output of the (by definition, single) current test
    member _.CaptureSerilogLog(testOutput) = serilogLog.CaptureSerilogLog testOutput
    member _.DumpStats() = stats.DumpStats()

    interface IDisposable with

        /// Stops the projector, emitting the final stats to the log
        member x.Dispose() =
            source.Stop()
            sink.Stop()
            x.DumpStats()
            (serilogLog :> IDisposable).Dispose()

[<Xunit.CollectionDefinition "CosmosReactor">]
type CosmosReactorCollection() =
    interface Xunit.ICollectionFixture<CosmosReactorFixture>
