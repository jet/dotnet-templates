namespace Reactor.Integration

open Infrastructure
open Propulsion.Internal
open Reactor
open System

module Guid =
    
    let generateStringN () = Guid.NewGuid() |> Domain.Guid.toString

/// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and the source passed from the concrete fixture 
/// See SerilogLogFixture for details of how to expose complete diagnostic messages
type FixtureBase(messageSink, store, dumpStats, createSourceConfig) =
    let serilogLog = new SerilogLogFixture(messageSink) // create directly to ensure correct sequencing and no loss of messages
    let contextId = Guid.generateStringN ()
    let handler = Handler.create store
    let log = Serilog.Log.Logger
    let stats = Handler.Stats(log, statsInterval = TimeSpan.FromMinutes 1, stateInterval = TimeSpan.FromMinutes 2,
                              logExternalStats = dumpStats)
    let sink = Handler.Config.StartSink(log, stats, handler, maxReadAhead = 1024, maxConcurrentStreams = 4,
                                        // Ensure batches are completed ASAP so waits in the tests are minimal
                                        wakeForResults = true)
    let source, awaitReactions =
        let consumerGroupName = $"ReactorFixture/{contextId}"
        let sourceConfig = createSourceConfig consumerGroupName
        Handler.Config.StartSource(log, sink, sourceConfig)

    member val Store = store

    /// As this is a Collection Fixture, it will outlive an individual instantiation of a Test Class
    /// This enables us to tee the output that normally goes to the Test Runner Diagnostic Sink to the test output of the (by definition, single) current test
    member _.CaptureSerilogLog(testOutput) = serilogLog.CaptureSerilogLog testOutput
    member _.DumpStats() =
        if stats.StatsInterval.RemainingMs > 3000 then
            stats.StatsInterval.Trigger()
            stats.StatsInterval.SleepUntilTriggerCleared()
    member _.Await(propagationDelay) =
        match awaitReactions with
        | Some f -> f propagationDelay |> Async.ofTask
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
        new(messageSink) =
            let store = Equinox.MemoryStore.VolatileStore()
            let createSourceConfig _groupName = SourceConfig.Memory store
            new Fixture(messageSink, Domain.Config.Store.Memory store, createSourceConfig)
        // override _.RunTimeout = TimeSpan.FromSeconds 0.1
        member _.Wait() = base.Await(TimeSpan.MaxValue) // Propagation delay is not applicable for MemoryStore
        member val private Backoff = TimeSpan.FromMilliseconds 1
        member val private Timeout = if System.Diagnostics.Debugger.IsAttached then TimeSpan.FromHours 1. else TimeSpan.FromSeconds 5.
        // Can be increased to only note long delays, but in general it's more useful to see the phases of processing 
        member val private WarnThreshold = TimeSpan.Zero
        member x.CheckReactions label = Propulsion.Reactor.Monitor.check x.Wait x.Backoff x.Timeout x.WarnThreshold label

module DynamoReactor =

    let tailSleepInterval = TimeSpan.FromMilliseconds 50

    /// XUnit Collection Fixture managing setup and disposal of Serilog.Log.Logger, a Reactor instance and a Propulsion.DynamoStoreSource Feed
    type Fixture private (messageSink, store, dumpStats, createSource) =
        inherit FixtureBase(messageSink, store, dumpStats, createSource)
        new (messageSink) =
            let conn = DynamoConnector()
            let createSourceConfig consumerGroupName =
                let loadMode = DynamoLoadModeConfig.NoBodies
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
