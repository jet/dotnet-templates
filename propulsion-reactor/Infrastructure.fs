﻿[<AutoOpen>]
module ReactorTemplate.Infrastructure

// #if (kafka || !blank)
open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
// #endif
open Serilog
open System

// #if (kafka || !blank)
module Guid =

    let inline toStringN (x: Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value: ClientId): string = Guid.toStringN %value
    let parse (value: string): ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

// #endif
module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

// #if (kafka || !blank)
module Streams =

    let private renderBody (x: Propulsion.Sinks.EventBody) = System.Text.Encoding.UTF8.GetString(x.Span)
    // Uses the supplied codec to decode the supplied event record (iff at LogEventLevel.Debug, failures are logged, citing `stream` and `.Data`)
    let private tryDecode<'E> (codec: Propulsion.Sinks.Codec<'E>) (streamName: FsCodec.StreamName) event =
        match codec.TryDecode event with
        | ValueNone when Log.IsEnabled Serilog.Events.LogEventLevel.Debug ->
            Log.ForContext("eventData", renderBody event.Data)
                .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, event.EventType, streamName)
            ValueNone
        | x -> x
    let (|Decode|) codec struct (stream, events: Propulsion.Sinks.Event[]): 'E[] =
        events |> Propulsion.Internal.Array.chooseV (tryDecode codec stream)
    
    module Codec =
        
        let gen<'E when 'E :> TypeShape.UnionContract.IUnionContract> : Propulsion.Sinks.Codec<'E> =
            FsCodec.SystemTextJson.Codec.Create<'E>() // options = Options.Default

        let private withUpconverter<'c, 'e when 'c :> TypeShape.UnionContract.IUnionContract> up: Propulsion.Sinks.Codec<'e> =
            let down (_: 'e) = failwith "Unexpected"
            FsCodec.SystemTextJson.Codec.Create<'e, 'c, _>(up, down) // options = Options.Default
        let genWithIndex<'c when 'c :> TypeShape.UnionContract.IUnionContract> : Propulsion.Sinks.Codec<int64 * 'c>  =
            let up (raw: FsCodec.ITimelineEvent<_>) e = raw.Index, e
            withUpconverter<'c, int64 * 'c> up

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
    let create (storeClient: Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)
        
module Dynamo =

    open Equinox.DynamoStore
    
    let defaultCacheDuration = TimeSpan.FromMinutes 20.
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
#if !sourceKafka        
    member client.CreateCheckpointService(consumerGroupName, cache, log, ?checkpointInterval) =
        let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
        let context = Equinox.DynamoStore.DynamoStoreContext(client)
        Propulsion.Feed.ReaderCheckpoint.DynamoStore.create log (consumerGroupName, checkpointInterval) (context, cache)

#endif
type Equinox.DynamoStore.DynamoStoreContext with

    member internal x.LogConfiguration(log: ILogger) =
        log.Information("DynamoStore Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query Paging {queryMaxItems} items",
                        x.TipOptions.MaxBytes, Option.toNullable x.TipOptions.MaxEvents, x.QueryOptions.MaxItems)

type Amazon.DynamoDBv2.IAmazonDynamoDB with

    member x.ConnectStore(role, table) =
        let storeClient = Equinox.DynamoStore.DynamoStoreClient(x, table)
        storeClient.LogConfiguration(role)
        storeClient

module DynamoStoreContext =

    /// Create with default packing and querying policies. Search for other `module DynamoStoreContext` impls for custom variations
    let create (storeClient: Equinox.DynamoStore.DynamoStoreClient) =
        Equinox.DynamoStore.DynamoStoreContext(storeClient, queryMaxItems = 100)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
                    c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)

module Equinox_CosmosStore_Exceptions =
    open Microsoft.Azure.Cosmos
    let [<return: Struct>] (|CosmosStatus|_|) (x: exn) = match x with :? CosmosException as ce -> ValueSome ce.StatusCode | _ -> ValueNone
    let (|RateLimited|RequestTimeout|ServiceUnavailable|CosmosStatusCode|Other|) = function
        | CosmosStatus System.Net.HttpStatusCode.TooManyRequests ->     RateLimited
        | CosmosStatus System.Net.HttpStatusCode.RequestTimeout ->      RequestTimeout
        | CosmosStatus System.Net.HttpStatusCode.ServiceUnavailable ->  ServiceUnavailable
        | CosmosStatus s -> CosmosStatusCode s
        | _ -> Other

module OutcomeKind =
    
    let [<return: Struct>] (|StoreExceptions|_|) exn =
        match exn with
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded
        | Equinox_CosmosStore_Exceptions.RateLimited -> Propulsion.Streams.OutcomeKind.RateLimited |> ValueSome
        | Equinox_CosmosStore_Exceptions.RequestTimeout -> Propulsion.Streams.OutcomeKind.Timeout |> ValueSome
        | _ -> ValueNone
