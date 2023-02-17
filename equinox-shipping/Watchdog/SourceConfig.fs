namespace Shipping.Infrastructure

open System
open System.Threading.Tasks

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type SourceConfig =
    | Memory of store : Equinox.MemoryStore.VolatileStore<struct (int * ReadOnlyMemory<byte>)>
    | Cosmos of monitoredContainer : Microsoft.Azure.Cosmos.Container
        * leasesContainer : Microsoft.Azure.Cosmos.Container
        * checkpoints : CosmosFeedConfig
        * tailSleepInterval : TimeSpan
    | Dynamo of indexStore : Equinox.DynamoStore.DynamoStoreClient
        * checkpoints : Propulsion.Feed.IFeedCheckpointStore
        * loading : Propulsion.DynamoStore.EventLoadMode
        * startFromTail : bool
        * batchSizeCutoff : int
        * tailSleepInterval : TimeSpan
        * statsInterval : TimeSpan
    | Esdb of client : EventStore.Client.EventStoreClient
        * checkpoints : Propulsion.Feed.IFeedCheckpointStore
        * withData : bool
        * startFromTail : bool
        * batchSize : int
        * tailSleepInterval : TimeSpan
        * statsInterval : TimeSpan
and [<NoEquality; NoComparison>] CosmosFeedConfig =
    | Ephemeral of processorName : string
    | Persistent of processorName : string * startFromTail : bool * maxItems : int option * lagFrequency : TimeSpan

module SourceConfig =
    module Memory =
        open Propulsion.MemoryStore
        let start log (sink : Propulsion.Streams.Default.Sink) categories
            (store : Equinox.MemoryStore.VolatileStore<_>) : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source = MemoryStoreSource(log, store, (fun x -> Array.contains x categories), sink)
            source.Start(), Some (fun _propagationDelay -> source.Monitor.AwaitCompletion(ignoreSubsequent = false))
    module Cosmos =
        open Propulsion.CosmosStore
        let start log (sink : Propulsion.Streams.Default.Sink) categories
            (monitoredContainer, leasesContainer, checkpointConfig, tailSleepInterval) : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let parseFeedDoc = EquinoxSystemTextJsonParser.enumStreamEvents (fun x -> Array.contains x categories)
            let observer = CosmosStoreSource.CreateObserver(log, sink.StartIngester, Seq.collect parseFeedDoc)
            let source =
                match checkpointConfig with
                | Ephemeral processorName ->
                    let withStartTime1sAgo (x : Microsoft.Azure.Cosmos.ChangeFeedProcessorBuilder) =
                        x.WithStartTime(let t = DateTime.UtcNow in t.AddSeconds -1.)
                    let lagFrequency = TimeSpan.FromMinutes 1.
                    CosmosStoreSource.Start(log, monitoredContainer, leasesContainer, processorName, observer,
                                            startFromTail = true, customize = withStartTime1sAgo, tailSleepInterval = tailSleepInterval,
                                            lagReportFreq = lagFrequency)
                | Persistent (processorName, startFromTail, maxItems, lagFrequency) ->
                    CosmosStoreSource.Start(log, monitoredContainer, leasesContainer, processorName, observer,
                                            startFromTail = startFromTail, ?maxItems = maxItems, tailSleepInterval = tailSleepInterval,
                                            lagReportFreq = lagFrequency)
            source, None
    module Dynamo =
        open Propulsion.DynamoStore
        let create (log, storeLog) (sink : Propulsion.Streams.Default.Sink) categories
            (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) trancheIds =
            DynamoStoreSource(
                log, statsInterval,
                indexStore, batchSizeCutoff, tailSleepInterval,
                checkpoints, sink, loadMode, categories = categories,
                startFromTail = startFromTail, storeLog = storeLog, ?trancheIds = trancheIds)
        let start (log, storeLog) sink categories (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval)
            : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source = create (log, storeLog) sink categories (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) None
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))
    module Esdb =
        open Propulsion.EventStoreDb
        let start log (sink : Propulsion.Streams.Default.Sink) categories
            (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval) : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source =
                EventStoreSource(
                    log, statsInterval,
                    client, batchSize, tailSleepInterval,
                    checkpoints, sink, (fun x -> Array.contains x categories), withData = withData, startFromTail = startFromTail)
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))

    let start (log, storeLog) sink categories : SourceConfig -> Propulsion.Pipeline * (TimeSpan -> Task<unit>) option = function
        | SourceConfig.Memory volatileStore ->
            Memory.start log sink categories volatileStore
        | SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval) ->
            Cosmos.start log sink categories (monitored, leases, checkpointConfig, tailSleepInterval)
        | SourceConfig.Dynamo (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) ->
            Dynamo.start (log, storeLog) sink categories (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval)
        | SourceConfig.Esdb (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval) ->
            Esdb.start log sink categories (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval)
