[<AutoOpen>]
module ReactorTemplate.Infrastructure

// #if (kafka || !blank)
open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
// #endif
open Serilog
open System

// #if (kafka || !blank)
module Guid =

    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value : ClientId) : string = Guid.toStringN %value
    let parse (value : string) : ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

// #endif
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

// #if (kafka || !blank)
module EventCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IEventCodec<_, _, _>) streamName (x : FsCodec.ITimelineEvent<Propulsion.Streams.Default.EventBody>) =
        match codec.TryDecode x with
        | ValueNone ->
            if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                Log.ForContext("event", System.Text.Encoding.UTF8.GetString(let d = x.Data in d.Span), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, streamName)
            ValueNone
        | x -> x

// #endif
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

    /// Connect a CosmosStoreClient, including warming up
    member x.ConnectStore(connectionName, databaseId, containerId) =
        x.LogConfiguration(connectionName, databaseId, containerId)
        Equinox.CosmosStore.CosmosStoreClient.Connect(x.CreateAndInitialize, databaseId, containerId)

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)
        
module Dynamo =

    open Equinox.DynamoStore
    
    let defaultCacheDuration = System.TimeSpan.FromMinutes 20.
    let private createCached codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, FsCodec.Deflate.EncodeTryDeflate codec, fold, initial, cacheStrategy, accessStrategy)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached codec initial fold accessStrategy (context, cache)

type Equinox.DynamoStore.DynamoStoreConnector with

    member x.LogConfiguration() =
        Log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}",
                        x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)

type Equinox.DynamoStore.DynamoStoreClient with

    member internal x.LogConfiguration(role, ?log) =
        (defaultArg log Log.Logger).Information("DynamoStore {role:l} Table {table} Archive {archive}", role, x.TableName, Option.toObj x.ArchiveTableName)
    member client.CreateCheckpointService(consumerGroupName, cache, log, ?checkpointInterval) =
        let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
        let context = Equinox.DynamoStore.DynamoStoreContext(client)
        Propulsion.Feed.ReaderCheckpoint.DynamoStore.create log (consumerGroupName, checkpointInterval) (context, cache)

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
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {NewLine}{Exception}"
                    c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
