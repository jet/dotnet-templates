namespace TodoBackendTemplate.Web

open Microsoft.Extensions.DependencyInjection
open System
open TodoBackendTemplate

/// Equinox store bindings
module Storage =

    /// Specifies the store to be used, together with any relevant custom parameters
    [<RequireQualifiedAccess>]
    type Config =
//#if (memoryStore || (!cosmos && !dynamo && !eventStore))
        | Memory
//#endif
//#if eventStore
        | Esdb of connectionString: string * cacheMb: int
//#endif
//#if cosmos
        | Cosmos of mode: Microsoft.Azure.Cosmos.ConnectionMode * connectionStringWithUriAndKey: string * database: string * container: string * cacheMb: int
//#endif
//#if dynamo
        | Dynamo of region: string * tableName: string * cacheMb: int
//#endif

//#if (memoryStore || (!cosmos && !dynamo && !eventStore))
    /// MemoryStore 'wiring', uses Equinox.MemoryStore nuget package
    module private Memory =
        open Equinox.MemoryStore
        let connect () =
            VolatileStore()

//#endif
//#if eventStore
    /// EventStore wiring, uses Equinox.EventStoreDb nuget package
    module private ES =
        open Equinox.EventStoreDb
        let connect connectionString =
            let c = EventStoreConnector(reqTimeout=TimeSpan.FromSeconds 5.(*, reqRetries = 1*))
            let conn = c.Establish("Twin", Discovery.ConnectionString connectionString, ConnectionStrategy.ClusterTwinPreferSlaveReads)
            EventStoreContext(conn, batchSize = 500)

//#endif
//#if cosmos
    /// CosmosDb wiring, uses Equinox.CosmosStore nuget package
    module private Cosmos =
        let connect (mode, discovery, databaseId, containerId) (maxRetryForThrottling, maxRetryWait) =
            let conn = Equinox.CosmosStore.CosmosStoreConnector(discovery, maxRetryForThrottling, maxRetryWait, mode = mode)
            let client = conn.Connect(databaseId, [| containerId |]) |> Async.RunSynchronously
            Equinox.CosmosStore.CosmosStoreContext(client, databaseId, containerId, tipMaxEvents = 256)

//#endif
//#if dynamo
    /// DynamoDB wiring, uses Equinox.DynamoStore nuget package
    module private Dynamo =
        open Equinox.DynamoStore
        let connect (region, table) (timeout, retries) =
            let c = DynamoStoreConnector(region, timeout, retries).CreateDynamoStoreClient()
            DynamoStoreContext.Establish(c, table) |> Async.RunSynchronously

//#endif
    /// Creates and/or connects to a specific store as dictated by the specified config
    let connect = function
//#if (memoryStore || (!cosmos && !dynamo && !eventStore))
        | Config.Memory ->
            let store = Memory.connect()
            Store.Config.Memory store
//#endif
//#if eventStore
        | Config.Esdb (connectionString, cache) ->
            let cache = Equinox.Cache("ES", sizeMb = cache)
            let conn = ES.connect connectionString
            Store.Config.Esdb (conn, cache)
//#endif
//#if cosmos
        | Config.Cosmos (mode, connectionString, database, container, cache) ->
            let cache = Equinox.Cache("Cosmos", sizeMb = cache)
            let retriesOn429Throttling = 1 // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            let timeout = TimeSpan.FromSeconds 5. // Timeout applied per request to CosmosDb, including retry attempts
            let context = Cosmos.connect (mode, Equinox.CosmosStore.Discovery.ConnectionString connectionString, database, container) (retriesOn429Throttling, timeout)
            Store.Config.Cosmos (context, cache)
//#endif
//#if dynamo
        | Config.Dynamo (region, table, cache) ->
            let cache = Equinox.Cache("Dynamo", sizeMb = cache)
            let retries = 1 // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            let timeout = TimeSpan.FromSeconds 5. // Timeout applied per request, including retry attempts
            let context = Dynamo.connect (region, table) (timeout, retries)
            Store.Config.Dynamo (context, cache)
//#endif

/// Dependency Injection wiring for services using Equinox
module Services =

    /// Registers the Equinox Store, Stream Resolver, Service Builder and the Service
    let register (services: IServiceCollection, storage) =
        let store = Storage.connect storage
//#if todos
        services.AddSingleton(Todo.Factory.create store) |> ignore
//#endif
//#if aggregate
        services.AddSingleton(Aggregate.Factory.create store) |> ignore
//#else
        //services.AddSingleton(Thing.Config.create store) |> ignore
//#endif