namespace ReactorTemplate

open System
open System.Threading.Tasks

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type SourceConfig =
    | Cosmos of monitoredContainer: Microsoft.Azure.Cosmos.Container
        * leasesContainer: Microsoft.Azure.Cosmos.Container
        * checkpoints: CosmosFeedConfig
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
    | Dynamo of indexContext: Equinox.DynamoStore.DynamoStoreContext
        * checkpoints: Propulsion.Feed.IFeedCheckpointStore
        * loading: Propulsion.DynamoStore.EventLoadMode
        * startFromTail: bool
        * batchSizeCutoff: int
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
    | Esdb of client: EventStore.Client.EventStoreClient
        * checkpoints: Propulsion.Feed.IFeedCheckpointStore
        * withData: bool
        * startFromTail: bool
        * batchSize: int
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
    | Sss of client: SqlStreamStore.IStreamStore
        * checkpoints: Propulsion.Feed.IFeedCheckpointStore
        * withData: bool
        * startFromTail: bool
        * batchSize: int
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
and [<NoEquality; NoComparison>] CosmosFeedConfig =
    | Ephemeral of processorName: string
    | Persistent of processorName: string * startFromTail: bool * maxItems: int option * lagFrequency: TimeSpan

module SourceConfig =
    module Cosmos =
        open Propulsion.CosmosStore
        let start log (sink: Propulsion.Sinks.SinkPipeline) categories
            (monitoredContainer, leasesContainer, checkpointConfig, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) =
            let parseFeedDoc = EquinoxSystemTextJsonParser.ofCategories categories
            let source =
                match checkpointConfig with
                | Ephemeral processorName ->
                    let withStartTime1sAgo (x: Microsoft.Azure.Cosmos.ChangeFeedProcessorBuilder) =
                        x.WithStartTime(let t = DateTime.UtcNow in t.AddSeconds -1.)
                    let lagFrequency = TimeSpan.FromMinutes 1.
                    CosmosStoreSource(log, statsInterval, monitoredContainer, leasesContainer, processorName, parseFeedDoc, sink,
                                      startFromTail = true, customize = withStartTime1sAgo, tailSleepInterval = tailSleepInterval,
                                      lagEstimationInterval = lagFrequency).Start()
                | Persistent (processorName, startFromTail, maxItems, lagFrequency) ->
                    CosmosStoreSource(log, statsInterval, monitoredContainer, leasesContainer, processorName, parseFeedDoc, sink,
                                      startFromTail = startFromTail, ?maxItems = maxItems, tailSleepInterval = tailSleepInterval,
                                      lagEstimationInterval = lagFrequency).Start()
            source, fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false)
    module Dynamo =
        open Propulsion.DynamoStore
        let start (log, storeLog) (sink: Propulsion.Sinks.SinkPipeline) categories
            (indexContext, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) =
            let source =
                DynamoStoreSource(
                    log, statsInterval,
                    indexContext, batchSizeCutoff, tailSleepInterval,
                    checkpoints, sink, loadMode, categories = categories,
                    startFromTail = startFromTail, storeLog = storeLog)
            let source = source.Start()
            source, fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false)
    module Esdb =
        open Propulsion.EventStoreDb
        let start log (sink: Propulsion.Sinks.SinkPipeline) categories
            (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) =
            let source =
                EventStoreSource(
                    log, statsInterval,
                    client, batchSize, tailSleepInterval,
                    checkpoints, sink, categories, withData = withData, startFromTail = startFromTail)
            let source = source.Start()
            source, fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false)
    module Sss =
        open Propulsion.SqlStreamStore
        let start log (sink: Propulsion.Sinks.SinkPipeline) categories
            (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) =
            let source =
                SqlStreamStoreSource(
                    log, statsInterval,
                    client, batchSize, tailSleepInterval,
                    checkpoints, sink, categories, withData = withData, startFromTail = startFromTail)
            let source = source.Start()
            source, fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false)
            
    let start (log, storeLog) sink categories: SourceConfig -> Propulsion.Pipeline * (TimeSpan -> Task<unit>) = function
        | SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval, statsInterval) ->
            Cosmos.start log sink categories (monitored, leases, checkpointConfig, tailSleepInterval, statsInterval)
        | SourceConfig.Dynamo (indexContext, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) ->
            Dynamo.start (log, storeLog) sink categories (indexContext, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval)
        | SourceConfig.Esdb (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval) ->
            Esdb.start log sink categories (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval)
        | SourceConfig.Sss (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval) ->
            Sss.start log sink categories (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval)
