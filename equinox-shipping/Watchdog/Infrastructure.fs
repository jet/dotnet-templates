[<AutoOpen>]
module Infrastructure

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
        |> fun c -> let t = "{Timestamp:HH:mm:ss} {Level:u1} {Message:lj} {SourceContext} {Properties}{NewLine}{Exception}"
                    c.WriteTo.Console(theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate = t)

module CosmosStoreConnector =

    let private get (role: string) (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
        Log.Information("CosmosDB {role} Database {database} Container {container}", role, databaseId, containerId)
        client.GetDatabase(databaseId).GetContainer(containerId)
    let getSource = get "Source"
    let getLeases = get "Leases"
    let getSourceAndLeases client databaseId containerId auxContainerId =
        getSource client databaseId containerId, getLeases client databaseId auxContainerId

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role: string, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents)
        c.LogConfiguration(role, databaseId, containerId)
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
    member private x.Connect(role, databaseId, containerId: string, ?auxContainerId) = async {
        let! cosmosClient = x.CreateAndInitialize(role, databaseId, [| containerId; yield! Option.toList auxContainerId |])
        return cosmosClient, Equinox.CosmosStore.CosmosStoreClient(cosmosClient).CreateContext(role, databaseId, containerId, tipMaxEvents = 256) }
    member x.ConnectContext(databaseId, containerId: string) = async {
        let! _client, context = x.Connect("Main", databaseId, containerId)
        return context }
    member x.ConnectWithFeed(databaseId, containerId, auxContainerId) = async {
        let! client, context = x.Connect("Main", databaseId, containerId, auxContainerId)
        let source, leases = CosmosStoreConnector.getSourceAndLeases client databaseId containerId auxContainerId
        return context, source, leases }

type Equinox.DynamoStore.DynamoStoreConnector with

    member x.LogConfiguration() =
        Log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}",
                        x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)

    member x.CreateClient() =
        x.LogConfiguration()
        x.CreateDynamoDbClient()
        |> Equinox.DynamoStore.DynamoStoreClient

type Equinox.DynamoStore.DynamoStoreClient with

    member x.CreateContext(role, table, ?queryMaxItems, ?maxBytes, ?archiveTableName: string) =
        let queryMaxItems = defaultArg queryMaxItems 100
        let c = Equinox.DynamoStore.DynamoStoreContext(x, table, queryMaxItems = queryMaxItems, ?maxBytes = maxBytes, ?archiveTableName = archiveTableName)
        Log.Information("DynamoStore {role:l} Table {table} Archive {archive} Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query paging {queryMaxItems} items",
                        role, table, Option.toObj archiveTableName, c.TipOptions.MaxBytes, Option.toNullable c.TipOptions.MaxEvents, c.QueryOptions.MaxItems)
        c
        
type Equinox.DynamoStore.DynamoStoreContext with

    member context.CreateCheckpointService(consumerGroupName, cache, log, ?checkpointInterval) =
        let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
        Propulsion.Feed.ReaderCheckpoint.DynamoStore.create log (consumerGroupName, checkpointInterval) (context, cache)

module EventStoreContext =

    let create (storeConnection: Equinox.EventStoreDb.EventStoreConnection) =
        Equinox.EventStoreDb.EventStoreContext(storeConnection, batchSize = 200)

 module OutcomeKind =
    
    let [<return: Struct>] (|StoreExceptions|_|) exn =
        match exn with
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded
        | Equinox.CosmosStore.Exceptions.RateLimited -> Propulsion.Streams.OutcomeKind.RateLimited |> ValueSome
        | Equinox.CosmosStore.Exceptions.RequestTimeout -> Propulsion.Streams.OutcomeKind.Timeout |> ValueSome
        | _ -> ValueNone
