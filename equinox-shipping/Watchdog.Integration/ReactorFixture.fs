namespace Shipping.Watchdog.Integration

open Shipping.Watchdog
open System

/// Manages creation, log capture and correct shutdown (inc dumping stats) of a Reactor instance over a Memory Store
/// Not a Class Fixture as a) it requires TestOutput b) we want to guarantee no residual state from preceding tests (esp if they timed out)
type MemoryReactorFixture(testOutput) =
    let store = new MemoryStoreFixture()
    let manager =
        let maxDop = 4
        Shipping.Domain.FinalizationProcess.Config.create maxDop store.Config
        
    let log = XunitLogger.forTest testOutput
    let stats = Handler.Config.CreateStats(log, statsInterval = TimeSpan.FromSeconds 10., stateInterval = TimeSpan.FromMinutes 1., storeVerbose = false)
    let sink = Handler.Config.StartSink(stats, log, manager, processingTimeout = TimeSpan.FromSeconds 1., maxReadAhead = 4096, maxConcurrentStreams = 4)

    let source = MemoryStoreProjector.Start(log, sink)
    let buildProjector filter =
        let mapTimelineEvent =
            let mapBodyToBytes = (fun (x : ReadOnlyMemory<byte>) -> x.ToArray())
            FsCodec.Core.TimelineEvent.Map (FsCodec.Deflate.EncodedToUtf8 >> mapBodyToBytes) // TODO replace with FsCodec.Deflate.EncodedToByteArray
        let mapBody (s, e) = s, e |> Array.map mapTimelineEvent
        let projectorStoreSubscription =
            match store.Config with
            | Shipping.Domain.Config.Store.Memory store -> source.Subscribe(store.Committed |> Observable.map mapBody |> Observable.filter (fst >> filter))
            | x -> failwith $"unexpected store config %A{x}"
        projectorStoreSubscription
    let storeSub = buildProjector Handler.isReactionStream

    member val ProcessManager = manager
    member val RunTimeout = TimeSpan.FromSeconds 0.1
    member val Log = log

    /// Waits until all <c>Submit</c>ted batches have been fed through the <c>inner</c> Projector
    member _.AwaitWithStopOnCancellation() = source.AwaitWithStopOnCancellation()

    interface IDisposable with

        /// Stops the projector, emitting the final stats to the log
        member _.Dispose() =
            (store :> IDisposable).Dispose()
            storeSub.Dispose()
            stats.DumpStats()

/// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and the source passed from the concrete fixture 
/// See SerilogLogFixture for details of how to expose complete diagnostic messages
type FixtureBase(messageSink, store, sourceConfig) =
    let serilogLog = new SerilogLogFixture(messageSink) // create directly to ensure correct sequencing and no loss of messages

    let manager =
        let maxDop = 4
        Shipping.Domain.FinalizationProcess.Config.create maxDop store
    let log = Serilog.Log.Logger
    let stats = Handler.Config.CreateStats(log, statsInterval = TimeSpan.FromSeconds 10., stateInterval = TimeSpan.FromMinutes 2., storeVerbose = true)
    let sink = Handler.Config.StartSink(stats, log, manager, processingTimeout = TimeSpan.FromSeconds 1., maxReadAhead = 1024, maxConcurrentStreams = 4)
    let source =
        let consumerGroupName = "ReactorFixture:" + Shipping.Domain.Guid.generateStringN ()
        let sourceConfig = sourceConfig consumerGroupName
        Handler.Config.StartSource(log, sink, sourceConfig)

    member val Store = store
    member val ProcessManager = manager
    member val RunTimeout = TimeSpan.FromSeconds 1.
    member val Log = Serilog.Log.Logger // initialized by CaptureSerilogLog

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

module CosmosReactor =

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.CosmosStore CFP
    type Fixture(messageSink, store, createSourceConfig) =
        inherit FixtureBase(messageSink, store, createSourceConfig)
        new (messageSink) =
            let cosmos = CosmosConnector()
            let store, monitored = cosmos.Connect()
            let leases = cosmos.ConnectLeases()
            let sourceConfig groupName =
                let checkpointConfig = CosmosCheckpointConfig.Ephemeral groupName
                SourceConfig.Cosmos (monitored, leases, checkpointConfig)
            new Fixture(messageSink, store, sourceConfig)

    let [<Literal>] Name = "CosmosReactor"

    [<Xunit.CollectionDefinition(Name)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>

module DynamoReactor =

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.DynamoStoreSource Feed
    type Fixture(messageSink, store, createSource) =
        inherit FixtureBase(messageSink, store, createSource)
        new (messageSink) =
            let conn = DynamoConnections()
            let sourceConfig groupName =
                let checkpointInterval = TimeSpan.FromHours 1.
                let checkpoints = Propulsion.Feed.ReaderCheckpoint.DynamoStore.create Shipping.Domain.Config.log (groupName, checkpointInterval) conn.DynamoStore
                let loadMode = DynamoLoadModeConfig.Hydrate (conn.StoreContext, 2)
                let startFromTail, batchSizeCutoff = true, 100
                SourceConfig.Dynamo (conn.IndexClient, checkpoints, loadMode, startFromTail, batchSizeCutoff, statsInterval = TimeSpan.FromSeconds 10.)
            new Fixture(messageSink, conn.Store, sourceConfig)

    let [<Literal>] Name = "DynamoReactor"

    [<Xunit.CollectionDefinition(Name)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>
