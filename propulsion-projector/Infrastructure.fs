[<AutoOpen>]
module ProjectorTemplate.Infrastructure

open Serilog
open System

module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
                    c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                    
// #if (cosmos || esdb || sss)
module CosmosStoreConnector =

    let private get (role: string) (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
        Log.Information("CosmosDB {role} Database {database} Container {container}", role, databaseId, containerId)
        client.GetDatabase(databaseId).GetContainer(containerId)
    let getSource = get "Source"
    let getLeases = get "Leases"
    let getSourceAndLeases client databaseId containerId auxContainerId =
        getSource client databaseId containerId, getLeases client databaseId auxContainerId
        
type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(role, databaseId: string, containers: string[]) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDB {role} {mode} {endpointUri} {db} {containers} timeout {timeout}s Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        role, o.ConnectionMode, x.Endpoint, databaseId, containers, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
    member private x.CreateAndInitialize(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.CreateAndInitialize(databaseId, containers)
    member x.ConnectFeed(databaseId, containerId, auxContainerId, ?role) = async {
        let! cosmosClient = x.CreateAndInitialize(defaultArg role "Source", databaseId, [| containerId; auxContainerId|])
        return CosmosStoreConnector.getSourceAndLeases cosmosClient databaseId containerId auxContainerId }

// #endif
// #if (dynamo || esdb || sss)
module Dynamo =

    open Equinox.DynamoStore
    
    let defaultCacheDuration = TimeSpan.FromMinutes 20.
    let private createCached name codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)
        DynamoStoreCategory(context, name, FsCodec.Deflate.EncodeTryDeflate codec, fold, initial, accessStrategy, cacheStrategy)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context, cache)

type Equinox.DynamoStore.DynamoStoreConnector with

    member private x.LogConfiguration() =
        Log.Information("DynamoDB {endpoint} Timeout {timeoutS}s Retries {retries}",
                        x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)
        
    member x.CreateClient() =
        x.LogConfiguration()
        x.CreateDynamoDbClient()
        |> Equinox.DynamoStore.DynamoStoreClient

type Equinox.DynamoStore.DynamoStoreClient with

    member x.CreateContext(role, table, ?queryMaxItems, ?maxBytes, ?archiveTableName: string) =
        let c = Equinox.DynamoStore.DynamoStoreContext(x, table, ?queryMaxItems = queryMaxItems, ?maxBytes = maxBytes, ?archiveTableName = archiveTableName)
        Log.Information("DynamoStore {role:l} Table {table} Archive {archive} Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query paging {queryMaxItems} items",
                        role, table, Option.toObj archiveTableName, c.TipOptions.MaxBytes, Option.toNullable c.TipOptions.MaxEvents, c.QueryOptions.MaxItems)
        c

type Equinox.DynamoStore.DynamoStoreContext with

    member context.CreateCheckpointService(consumerGroupName, cache, log, ?checkpointInterval) =
        let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
        Propulsion.Feed.ReaderCheckpoint.DynamoStore.create log (consumerGroupName, checkpointInterval) (context, cache)

// #endif
