[<AutoOpen>]
module Shipping.Infrastructure

open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Exception =

    let dump verboseStore (log : ILogger) (exn : exn) =
        match exn with // TODO provide override option?
        | :? Microsoft.Azure.Cosmos.CosmosException as e
            when (e.StatusCode = System.Net.HttpStatusCode.TooManyRequests
                  || e.StatusCode = System.Net.HttpStatusCode.ServiceUnavailable)
                 && not verboseStore -> ()
        
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded
        | :? TimeoutException when not verboseStore -> ()
        
        | _ ->
            log.Information(exn, "Unhandled")

type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(connectionName, databaseId, containerId) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDb {name} {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        connectionName, o.ConnectionMode, x.Endpoint, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
        Log.Information("CosmosDb {name} Database {database} Container {container}",
                        connectionName, databaseId, containerId)

    /// Use sparingly; in general one wants to use CreateAndInitialize to avoid slow first requests
    member x.CreateUninitialized(databaseId, containerId) =
        x.CreateUninitialized().GetDatabase(databaseId).GetContainer(containerId)

    /// Creates a CosmosClient suitable for running a CFP via CosmosStoreSource
    member private x.ConnectMonitored(databaseId, containerId, ?connectionName) =
        x.LogConfiguration(defaultArg connectionName "Source", databaseId, containerId)
        x.CreateUninitialized(databaseId, containerId)

    /// Connects to a Store as both a ChangeFeedProcessor Monitored Container and a CosmosStoreClient
    member x.ConnectStoreAndMonitored(databaseId, containerId) =
        let monitored = x.ConnectMonitored(databaseId, containerId, "Main")
        let storeClient = Equinox.CosmosStore.CosmosStoreClient(monitored.Database.Client, databaseId, containerId)
        storeClient, monitored

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents = maxEvents)

type Equinox.DynamoStore.DynamoStoreConnector with

    member x.LogConfiguration() =
        Log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}",
                        x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)

type Equinox.DynamoStore.DynamoStoreClient with

    member internal x.LogConfiguration(role, ?log) =
        (defaultArg log Log.Logger).Information("DynamoStore {role:l} Table {table} Archive {archive}", role, x.TableName, Option.toObj x.ArchiveTableName)
    member client.CreateCheckpointService(consumerGroupName, cache, ?checkpointInterval) =
        let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
        let context = Equinox.DynamoStore.DynamoStoreContext(client)
        Propulsion.Feed.ReaderCheckpoint.DynamoStore.create Shipping.Domain.Config.log (consumerGroupName, checkpointInterval) (context, cache)

type Equinox.DynamoStore.DynamoStoreContext with

    member internal x.LogConfiguration(log : ILogger) =
        log.Information("DynamoStore Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query Paging {queryMaxItems} items",
                        x.TipOptions.MaxBytes, Option.toNullable x.TipOptions.MaxEvents, x.QueryOptions.MaxItems)

type Amazon.DynamoDBv2.IAmazonDynamoDB with

    member x.ConnectStore(role, table) =
        let storeClient = Equinox.DynamoStore.DynamoStoreClient(x, table)
        storeClient.LogConfiguration(role)
        storeClient

module DynamoStoreContext =

    /// Create with default packing and querying policies. Search for other `module DynamoStoreContext` impls for custom variations
    let create (storeClient : Equinox.DynamoStore.DynamoStoreClient) =
        Equinox.DynamoStore.DynamoStoreContext(storeClient, queryMaxItems = 100)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let t = "{Timestamp:HH:mm:ss} {Level:u1} {Message:lj} {SourceContext} {Properties}{NewLine}{Exception}"
                    c.WriteTo.Console(theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate = t)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type SourceConfig =
    | Memory of
        store : Equinox.MemoryStore.VolatileStore<struct (int * ReadOnlyMemory<byte>)>
    | Cosmos of
        monitoredContainer : Microsoft.Azure.Cosmos.Container
        * leasesContainer : Microsoft.Azure.Cosmos.Container
        * checkpoints : CosmosCheckpointConfig
    | Dynamo of
        indexStore : Equinox.DynamoStore.DynamoStoreClient
        * checkpoints : Propulsion.Feed.IFeedCheckpointStore
        * loading : DynamoLoadModeConfig
        * startFromTail : bool
        * batchSizeCutoff : int
        * tailSleepInterval : TimeSpan
        * statsInterval : TimeSpan
and CosmosCheckpointConfig =
    | Ephemeral of processorName : string
    | Persistent of processorName : string * startFromTail : bool * maxItems : int option * lagFrequency : TimeSpan
and [<NoEquality; NoComparison>]
    DynamoLoadModeConfig =
    | Hydrate of monitoredContext : Equinox.DynamoStore.DynamoStoreContext * hydrationConcurrency : int
    
module SourceConfig =
    module Memory =
        open Propulsion.MemoryStore
        let start log (sink : Propulsion.Streams.Default.Sink) streamFilter
            (store : Equinox.MemoryStore.VolatileStore<_>) : Propulsion.Pipeline * (TimeSpan -> Async<unit>) option =
            let source = MemoryStoreSource(log, store, streamFilter, sink)
            source.Start(), Some (fun _propagationDelay -> source.Monitor.AwaitCompletion(ignoreSubsequent = false))
    module Cosmos =
        open Propulsion.CosmosStore
        let start log (sink : Propulsion.Streams.Default.Sink) streamFilter
            (monitoredContainer, leasesContainer, checkpointConfig) : Propulsion.Pipeline * (TimeSpan -> Async<unit>) option =
            let parseFeedDoc = EquinoxSystemTextJsonParser.enumStreamEvents streamFilter
            let observer = CosmosStoreSource.CreateObserver(log, sink.StartIngester, Seq.collect parseFeedDoc)
            let source =
                match checkpointConfig with
                | Ephemeral processorName ->
                    let withStartTime1sAgo (x : Microsoft.Azure.Cosmos.ChangeFeedProcessorBuilder) =
                        x.WithStartTime(let t = DateTime.UtcNow in t.AddSeconds -1.)
                    let lagFrequency = TimeSpan.FromMinutes 1.
                    CosmosStoreSource.Start(log, monitoredContainer, leasesContainer, processorName, observer, startFromTail = false,
                                            customize = withStartTime1sAgo, lagReportFreq = lagFrequency)
                | Persistent (processorName, startFromTail, maxItems, lagFrequency) ->
                    CosmosStoreSource.Start(log, monitoredContainer, leasesContainer, processorName, observer, startFromTail,
                                            ?maxItems = maxItems, lagReportFreq = lagFrequency)
            source, None
    module Dynamo =
        open Propulsion.DynamoStore
        let start (log, storeLog) (sink : Propulsion.Streams.Default.Sink) streamFilter
            (indexStore, checkpoints, loadModeConfig, startFromTail, tailSleepInterval, batchSizeCutoff, statsInterval)
            : Propulsion.Pipeline * (TimeSpan -> Async<unit>) option =
            let loadMode =
                match loadModeConfig with
                | Hydrate (monitoredContext, hydrationConcurrency) -> LoadMode.Hydrated (streamFilter, hydrationConcurrency, monitoredContext)
            let source =
                DynamoStoreSource(
                    log, statsInterval,
                    indexStore, batchSizeCutoff, tailSleepInterval,
                    checkpoints, sink, loadMode, fromTail = startFromTail, storeLog = storeLog)
                    // trancheIds = [|Propulsion.Feed.TrancheId.parse "0"|]) // TEMP filter for additional clones of index data in target Table
            source.Start(), Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))

    let start (log, storeLog) (sink : Propulsion.Streams.Default.Sink) streamFilter
        : SourceConfig -> Propulsion.Pipeline * (TimeSpan -> Async<unit>) option = function
        | SourceConfig.Memory volatileStore ->
            Memory.start log sink streamFilter volatileStore
        | SourceConfig.Cosmos (monitored, leases, checkpointConfig) ->
            Cosmos.start log sink streamFilter (monitored, leases, checkpointConfig)
        | SourceConfig.Dynamo (indexStore, checkpoints, loading, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) ->
            Dynamo.start (log, storeLog) sink streamFilter (indexStore, checkpoints, loading, startFromTail, tailSleepInterval, batchSizeCutoff, statsInterval)
