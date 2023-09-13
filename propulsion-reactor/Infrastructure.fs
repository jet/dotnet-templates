[<AutoOpen>]
module ReactorTemplate.Infrastructure

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open Serilog
open System

module Guid =

    let inline toStringN (x: Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value: ClientId): string = Guid.toStringN %value
    let parse (value: string): ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

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
module CosmosStoreConnector =

    let private get (role: string) (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
        Log.Information("CosmosDB {role} Database {database} Container {container}", role, databaseId, containerId)
        client.GetDatabase(databaseId).GetContainer(containerId)
    let getSource = get "Source"
    let getLeases = get "Leases"
    let getSourceAndLeases client databaseId containerId auxContainerId =
        getSource client databaseId containerId, getLeases client databaseId auxContainerId

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents, ?queryMaxItems, ?tipMaxJsonLength, ?skipLog) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents, ?queryMaxItems = queryMaxItems, ?tipMaxJsonLength = tipMaxJsonLength)
        if skipLog = Some true then () else c.LogConfiguration(role, databaseId, containerId)
        c

type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(role, databaseId: string, containers: string[]) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDB {role} {mode} {endpointUri} {db} {containers} timeout {timeout}s Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        role, o.ConnectionMode, x.Endpoint, databaseId, containers, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
    member private x.CreateAndInitialize(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.CreateAndInitialize(databaseId, containers)
    member private x.Connect(role, databaseId, containerId, ?auxContainerId) = async {
        let! cosmosClient = x.CreateAndInitialize(role, databaseId, [| yield containerId; yield! Option.toList auxContainerId |])
        return cosmosClient, Equinox.CosmosStore.CosmosStoreClient(cosmosClient).CreateContext(role, databaseId, containerId, tipMaxEvents = 256) }
    member x.ConnectContext(role, databaseId, containerId: string) = async {
        let! _cosmosClient, context = x.Connect(role, databaseId, containerId)
        return context }
    member x.ConnectWithFeed(databaseId, containerId, auxContainerId) = async {
        let! cosmosClient, context = x.Connect("Main", databaseId, containerId, auxContainerId)
        let source, leases = CosmosStoreConnector.getSourceAndLeases cosmosClient databaseId containerId auxContainerId
        return context, source, leases }
        
module Dynamo =

    open Equinox.DynamoStore
    
    let defaultCacheDuration = TimeSpan.FromMinutes 20.
    let private createCached name codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, name, FsCodec.Compression.EncodeTryCompress codec, fold, initial, accessStrategy, cacheStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

type Equinox.DynamoStore.DynamoStoreConnector with

    member x.LogConfiguration() =
        Log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}", x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)
        
    member x.CreateClient() =
        x.LogConfiguration()
        x.CreateDynamoStoreClient()

type Equinox.DynamoStore.DynamoStoreClient with

    member x.CreateContext(role, table, ?queryMaxItems, ?maxBytes, ?archiveTableName: string) =
        let queryMaxItems = defaultArg queryMaxItems 100
        let c = Equinox.DynamoStore.DynamoStoreContext(x, table, queryMaxItems = queryMaxItems, ?maxBytes = maxBytes, ?archiveTableName = archiveTableName)
        Log.Information("DynamoStore {role:l} Table {table} Archive {archive} Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query paging {queryMaxItems} items",
                        role, table, Option.toObj archiveTableName, c.TipOptions.MaxBytes, Option.toNullable c.TipOptions.MaxEvents, c.QueryOptions.MaxItems)
        c
        
#if !sourceKafka        
type Equinox.DynamoStore.DynamoStoreContext with

     member context.CreateCheckpointService(consumerGroupName, cache, log, ?checkpointInterval) =
         let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
         Propulsion.Feed.ReaderCheckpoint.DynamoStore.create log (consumerGroupName, checkpointInterval) (context, cache)

#endif
[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
                    c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)

module OutcomeKind =
    
    let [<return: Struct>] (|StoreExceptions|_|) exn =
        match exn with
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded
        | Equinox.CosmosStore.Exceptions.RateLimited -> Propulsion.Streams.OutcomeKind.RateLimited |> ValueSome
        | Equinox.CosmosStore.Exceptions.RequestTimeout -> Propulsion.Streams.OutcomeKind.Timeout |> ValueSome
        | _ -> ValueNone
