namespace Shipping.Watchdog.Integration

open Shipping.Watchdog
open System

/// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and the source passed from the concrete fixture 
/// See SerilogLogFixture for details of how to expose complete diagnostic messages
type FixtureBase(messageSink, store, createSourceConfig) =
    let serilogLog = new SerilogLogFixture(messageSink) // create directly to ensure correct sequencing and no loss of messages
    let contextId = Shipping.Domain.Guid.generateStringN ()
    let manager =
        let maxDop = 4
        Shipping.Domain.FinalizationProcess.Config.create maxDop store
    let log = Serilog.Log.Logger
    let stats = Handler.Stats(log, statsInterval = TimeSpan.FromSeconds 30., stateInterval = TimeSpan.FromMinutes 2., verboseStore = true)
    let sink = Handler.Config.StartSink(log, stats, manager, processingTimeout = TimeSpan.FromSeconds 1., maxReadAhead = 1024, maxConcurrentStreams = 4)
    let source =
        let consumerGroupName = $"ReactorFixture/{contextId}"
        let sourceConfig = createSourceConfig consumerGroupName
        Handler.Config.StartSource(log, sink, sourceConfig)

    member val Store = store
    member val ProcessManager = manager
    abstract member RunTimeout : TimeSpan with get
    default _.RunTimeout = TimeSpan.FromSeconds 1.
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

module MemoryReactor =

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a MemoryStoreSource
    type Fixture private (messageSink, store, createSourceConfig) =
        inherit FixtureBase(messageSink, store, createSourceConfig)
        new (messageSink) =
            let store = Equinox.MemoryStore.VolatileStore()
            let createSourceConfig _groupName = SourceConfig.Memory store
            new Fixture(messageSink, Shipping.Domain.Config.Store.Memory store, createSourceConfig)
        override _.RunTimeout = TimeSpan.FromSeconds 0.1

module CosmosReactor =

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.CosmosStore CFP
    type Fixture private (messageSink, store, createSourceConfig) =
        inherit FixtureBase(messageSink, store, createSourceConfig)
        new (messageSink) =
            let cosmos = CosmosConnector()
            let store, monitored = cosmos.Connect()
            let leases = cosmos.ConnectLeases()
            let createSourceConfig groupName =
                let checkpointConfig = CosmosCheckpointConfig.Ephemeral groupName
                SourceConfig.Cosmos (monitored, leases, checkpointConfig)
            new Fixture(messageSink, store, createSourceConfig)

    let [<Literal>] CollectionName = "CosmosReactor"

    [<Xunit.CollectionDefinition(CollectionName)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>

module DynamoReactor =

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.DynamoStoreSource Feed
    type Fixture private (messageSink, store, createSourceConfig) =
        inherit FixtureBase(messageSink, store, createSourceConfig)
        new (messageSink) =
            let conn = DynamoConnector()
            let createSourceConfig groupName =
                let checkpointInterval = TimeSpan.FromHours 1.
                let checkpoints = Propulsion.Feed.ReaderCheckpoint.DynamoStore.create Shipping.Domain.Config.log (groupName, checkpointInterval) conn.DynamoStore
                let loadMode = DynamoLoadModeConfig.Hydrate (conn.StoreContext, 2)
                let startFromTail, batchSizeCutoff = true, 100
                SourceConfig.Dynamo (conn.IndexClient, checkpoints, loadMode, startFromTail, batchSizeCutoff, statsInterval = TimeSpan.FromSeconds 30.)
            new Fixture(messageSink, conn.Store, createSourceConfig)

    let [<Literal>] CollectionName = "DynamoReactor"

    [<Xunit.CollectionDefinition(CollectionName)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>
