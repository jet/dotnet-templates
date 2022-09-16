namespace Shipping.Watchdog.Integration

open Propulsion.Internal // IntervalTimer etc
open Shipping.Domain.Tests
open Shipping.Infrastructure
open Shipping.Watchdog
open System

/// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and the source passed from the concrete fixture 
/// See SerilogLogFixture for details of how to expose complete diagnostic messages
type FixtureBase(messageSink, store, dumpStats, createSourceConfig) =
    let serilogLog = new SerilogLogFixture(messageSink) // create directly to ensure correct sequencing and no loss of messages
    let contextId = Shipping.Domain.Guid.generateStringN ()
    let manager =
        let maxDop = 4
        Shipping.Domain.FinalizationProcess.Config.create maxDop store
    let log = Serilog.Log.Logger
    let stats = Handler.Stats(log, statsInterval = TimeSpan.FromMinutes 1, stateInterval = TimeSpan.FromMinutes 2,
                              verboseStore = true, logExternalStats = dumpStats)
    let sink = Handler.Config.StartSink(log, stats, manager, processingTimeout = TimeSpan.FromSeconds 1., maxReadAhead = 1024, maxConcurrentStreams = 4,
                                        // Ensure batches are completed ASAP so waits in the tests are minimal
                                        wakeForResults = true)
    let source, awaitReactions =
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
    member _.DumpStats() =
        stats.StatsInterval.Trigger()
        // The processing loops run on 1s timers, so we busy-wait until they wake
        let wait = IntervalTimer(TimeSpan.FromSeconds 2)
        while stats.StatsInterval.RemainingMs > 3000 && not (wait.IfDueRestart()) do
            System.Threading.Thread.Sleep(5)
    member _.Await(propagationDelay) =
        match awaitReactions with
        | Some f -> f propagationDelay
        | None -> async { ()  }

    interface IDisposable with

        /// Stops the projector, emitting the final stats to the log
        member x.Dispose() =
            source.Stop()
            sink.Stop()
            (serilogLog :> IDisposable).Dispose()

module MemoryReactor =

    /// XUnit Class Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a MemoryStoreSource
    type Fixture private (messageSink, store, createSourceConfig) =
        inherit FixtureBase(messageSink, store, ignore, createSourceConfig)
        new (messageSink) =
            let store = Equinox.MemoryStore.VolatileStore()
            let createSourceConfig _groupName = SourceConfig.Memory store
            new Fixture(messageSink, Shipping.Domain.Config.Store.Memory store, createSourceConfig)
        override _.RunTimeout = TimeSpan.FromSeconds 0.1
        member _.Wait() = base.Await(TimeSpan.MaxValue) // Propagation delay is not applicable for MemoryStore
        member val private Backoff = TimeSpan.FromMilliseconds 1
        member val private Timeout = if System.Diagnostics.Debugger.IsAttached then TimeSpan.FromHours 1. else TimeSpan.FromSeconds 5.
        // Can be increased to only note long delays, but in general it's more useful to see the phases of processing 
        member val private WarnThreshold = TimeSpan.Zero
        member x.CheckReactions label = Propulsion.Reactor.Monitor.check x.Wait x.Backoff x.Timeout x.WarnThreshold label

module CosmosReactor =

    let tailSleepInterval = TimeSpan.FromMilliseconds 300

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.CosmosStore CFP
    type Fixture private (messageSink, store, createSourceConfig) =
        inherit FixtureBase(messageSink, store, ignore, createSourceConfig)
        new (messageSink) =
            let conn = CosmosConnector()
            let store, monitored = conn.Connect()
            let leases = conn.ConnectLeases()
            let createSourceConfig consumerGroupName =
                let checkpointConfig = CosmosFeedConfig.Ephemeral consumerGroupName
                SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval)
            new Fixture(messageSink, store, createSourceConfig)
        member _.NullWait(_arguments) = async.Zero () // We could wire up a way to await all tranches having caught up, but not implemented yet
        member val private Timeout = if System.Diagnostics.Debugger.IsAttached then TimeSpan.FromHours 1. else TimeSpan.FromMinutes 1.
        member val private Backoff = TimeSpan.FromMilliseconds 100 // Vary to adjust effect of too many retries on rate limiting
        // Can be increased to only note long delays, but in general it's more useful to see the phases of processing 
        member val private WarnThreshold = TimeSpan.Zero
        member x.CheckReactions label = Propulsion.Reactor.Monitor.check x.NullWait x.Backoff x.Timeout x.WarnThreshold label

    let [<Literal>] CollectionName = "CosmosReactor"

    [<Xunit.CollectionDefinition(CollectionName)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>

module DynamoReactor =

    let tailSleepInterval = TimeSpan.FromMilliseconds 50

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.DynamoStoreSource Feed
    type Fixture private (messageSink, store, dumpStats, createSource) =
        inherit FixtureBase(messageSink, store, dumpStats, createSource)
        new (messageSink) =
            let conn = DynamoConnector()
            let createSourceConfig consumerGroupName =
                let loadMode = DynamoLoadModeConfig.Hydrate (conn.StoreContext, 4)
                let checkpoints = conn.CreateCheckpointService(consumerGroupName)
                SourceConfig.Dynamo (conn.IndexClient, checkpoints, loadMode, startFromTail = true, batchSizeCutoff = 100,
                                     tailSleepInterval = tailSleepInterval, statsInterval = TimeSpan.FromSeconds 60.)
            new Fixture(messageSink, conn.Store, conn.DumpStats, createSourceConfig)
        member val private Timeout = if System.Diagnostics.Debugger.IsAttached then TimeSpan.FromHours 1. else TimeSpan.FromMinutes 1.
        // Give the events a chance to propagate through the Streams and Lambda before we start the wait
        member val private PropagationDelay = tailSleepInterval * 2. + TimeSpan.FromMilliseconds 1200.
        member x.Backoff = tailSleepInterval * 2. // Vary to adjust effect of too many retries on rate limiting
        // Can be increased to only note long delays, but in general it's more useful to see the phases of processing 
        member val private WarnThreshold = TimeSpan.Zero
        member x.Wait() = base.Await(x.PropagationDelay)
        member x.CheckReactions label = Propulsion.Reactor.Monitor.check x.Wait x.Backoff x.Timeout x.WarnThreshold label

    let [<Literal>] CollectionName = "DynamoReactor"

    [<Xunit.CollectionDefinition(CollectionName)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>

module EsdbReactor =

    let tailSleepInterval = TimeSpan.FromMilliseconds 50

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.EventStoreDb.EventStoreSource
    type Fixture private (messageSink, store, dumpStats, createSource) =
        inherit FixtureBase(messageSink, store, dumpStats, createSource)
        new (messageSink) =
            let conn = EsdbConnector()
            let createSourceConfig consumerGroupName =
                let checkpoints = conn.CreateCheckpointService(consumerGroupName)
                SourceConfig.Esdb (conn.EventStoreClient, checkpoints, hydrateBodies = true, startFromTail = true, batchSize = 100,
                                   tailSleepInterval = tailSleepInterval, statsInterval = TimeSpan.FromSeconds 60.)
            new Fixture(messageSink, conn.Store, conn.DumpStats, createSourceConfig)
        override _.RunTimeout = TimeSpan.FromSeconds 0.1
        member val private Timeout = if System.Diagnostics.Debugger.IsAttached then TimeSpan.FromHours 1. else TimeSpan.FromMinutes 1.
        member val private PropagationDelay = tailSleepInterval * 2. + TimeSpan.FromMilliseconds 300.
        member x.Backoff = tailSleepInterval * 2.
        // Can be increased to only note long delays, but in general it's more useful to see the phases of processing 
        member val private WarnThreshold = TimeSpan.Zero
        member x.Wait() = base.Await(x.PropagationDelay)
        member x.CheckReactions label = Propulsion.Reactor.Monitor.check x.Wait x.Backoff x.Timeout x.WarnThreshold label

    let [<Literal>] CollectionName = "EsdbReactor"

    [<Xunit.CollectionDefinition(CollectionName)>]
    type Collection() =
        interface Xunit.ICollectionFixture<Fixture>
