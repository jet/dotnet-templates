namespace ProjectorTemplate

open System
open System.Threading.Tasks

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type SourceConfig =
// #if (cosmos)    
    | Cosmos of monitoredContainer: Microsoft.Azure.Cosmos.Container
        * leasesContainer: Microsoft.Azure.Cosmos.Container
        * checkpoints: CosmosFeedConfig
        * tailSleepInterval: TimeSpan
// #endif
#if dynamo    
    | Dynamo of indexContext: Equinox.DynamoStore.DynamoStoreContext
        * checkpoints: Propulsion.Feed.IFeedCheckpointStore
        * loading: Propulsion.DynamoStore.EventLoadMode
        * startFromTail: bool
        * batchSizeCutoff: int
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
#endif        
#if esdb     
    | Esdb of client: EventStore.Client.EventStoreClient
        * checkpoints: Propulsion.Feed.IFeedCheckpointStore
        * withData: bool
        * startFromTail: bool
        * batchSize: int
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
#endif        
#if sss     
    | Sss of client: SqlStreamStore.IStreamStore
        * checkpoints: Propulsion.Feed.IFeedCheckpointStore
        * withData: bool
        * startFromTail: bool
        * batchSize: int
        * tailSleepInterval: TimeSpan
        * statsInterval: TimeSpan
#endif        
// #if cosmos
and [<NoEquality; NoComparison>] CosmosFeedConfig =
    | Ephemeral of processorName: string
    | Persistent of processorName: string * startFromTail: bool * maxItems: int option * lagFrequency: TimeSpan
// #endif

module SourceConfig =
// #if cosmos    
    module Cosmos =
        open Propulsion.CosmosStore
        let start log (sink: Propulsion.Sinks.Sink) categories
            (monitoredContainer, leasesContainer, checkpointConfig, tailSleepInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let parseFeedDoc = EquinoxSystemTextJsonParser.enumCategoryEvents categories
            let observer = CosmosStoreSource.CreateObserver(log, sink.StartIngester, Seq.collect parseFeedDoc)
            let source =
                match checkpointConfig with
                | Ephemeral processorName ->
                    let withStartTime1sAgo (x: Microsoft.Azure.Cosmos.ChangeFeedProcessorBuilder) =
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
// #endif            
#if dynamo    
    module Dynamo =
        open Propulsion.DynamoStore
        let start (log, storeLog) (sink: Propulsion.Sinks.Sink) categories
            (indexContext, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source =
                DynamoStoreSource(
                    log, statsInterval,
                    indexContext, batchSizeCutoff, tailSleepInterval,
                    checkpoints, sink, loadMode, categories = categories,
                    startFromTail = startFromTail, storeLog = storeLog)
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))
#endif            
#if esdb    
    module Esdb =
        open Propulsion.EventStoreDb
        let start log (sink: Propulsion.Sinks.Sink) (categories: string[])
            (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source =
                EventStoreSource(
                    log, statsInterval,
                    client, batchSize, tailSleepInterval,
                    checkpoints, sink, categories, withData = withData, startFromTail = startFromTail)
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))
#endif            
#if sss    
    module Sss =
        open Propulsion.SqlStreamStore
        let start log (sink: Propulsion.Sinks.Sink) (categories: string[])
            (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval): Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source =
                SqlStreamStoreSource(
                    log, statsInterval,
                    client, batchSize, tailSleepInterval,
                    checkpoints, sink, categories, withData = withData, startFromTail = startFromTail)
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))
#endif            
    let start (log, storeLog) sink categories: SourceConfig -> Propulsion.Pipeline * (TimeSpan -> Task<unit>) option = function
// #if cosmos    
        | SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval) ->
            Cosmos.start log sink categories (monitored, leases, checkpointConfig, tailSleepInterval)
// #endif
#if dynamo    
        | SourceConfig.Dynamo (indexContext, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) ->
            Dynamo.start (log, storeLog) sink categories (indexContext, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval)
#endif
#if esdb    
        | SourceConfig.Esdb (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval) ->
            Esdb.start log sink categories (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval)
#endif
#if sss    
        | SourceConfig.Sss (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval) ->
            Sss.start log sink categories (client, checkpoints, withData, startFromTail, batchSize, tailSleepInterval, statsInterval)
#endif
