namespace TodoBackendTemplate.Web

open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Prometheus
open Serilog
open System
open TodoBackendTemplate

/// Equinox store bindings
module Store =

    /// Specifies the store to be used, together with any relevant custom parameters
    [<RequireQualifiedAccess>]
    type Context =
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
    /// EventStore wiring, uses Equinox.EventStore nuget package
    module private ES =
        open Equinox.EventStoreDb
        let connect connectionString =
            let c = EventStoreConnector(reqTimeout=TimeSpan.FromSeconds 5., reqRetries=1)
            let conn = c.Establish("Twin", Discovery.ConnectionString connectionString, ConnectionStrategy.ClusterTwinPreferSlaveReads)
            EventStoreContext(conn, batchSize = 500)

//#endif
//#if cosmos
    /// CosmosDb wiring, uses Equinox.CosmosStore nuget package
    module private Cosmos =
        open Equinox.CosmosStore
        module CosmosStoreContext =

            /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
            let create (storeClient: CosmosStoreClient) =
                let maxEvents = 256
                CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

        let connect (mode, discovery, databaseId, containerId) (operationTimeout, maxRetryForThrottling, maxRetryWait) =
            let c = CosmosStoreConnector(discovery, operationTimeout, maxRetryForThrottling, maxRetryWait, mode)
            CosmosStoreClient.Connect(c.CreateAndInitialize, databaseId, containerId) |> Async.RunSynchronously |> CosmosStoreContext.create

//#endif
//#if dynamo
    /// DynamoDB wiring, uses Equinox.DynamoStore nuget package
    module private Dynamo =
        open Equinox.DynamoStore
        module DynamoStoreContext =

            /// Create with default packing and querying policies. Search for other `module DynamoStoreContext` impls for custom variations
            let create (storeClient: DynamoStoreClient) =
                let maxEvents = 256
                DynamoStoreContext(storeClient, tipMaxEvents = maxEvents)

        let connect (region, table) (timeout, retries) =
            let c = DynamoStoreConnector(region, timeout, retries)
            DynamoStoreClient.Establish(c.CreateClient(), table) |> Async.RunSynchronously |> DynamoStoreContext.create

//#endif
    /// Creates and/or connects to a specific store as dictated by the specified config
    let connect = function
//#if (memoryStore || (!cosmos && !dynamo && !eventStore))
        | Context.Memory ->
            let store = Memory.connect()
            Store.Context.Memory store
//#endif
//#if eventStore
        | Context.Esdb (connectionString, cache) ->
            let cache = Equinox.Cache("ES", sizeMb = cache)
            let conn = ES.connect connectionString
            Store.Context.Esdb (conn, cache)
//#endif
//#if cosmos
        | Context.Cosmos (mode, connectionString, database, container, cache) ->
            let cache = Equinox.Cache("Cosmos", sizeMb = cache)
            let retriesOn429Throttling = 1 // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            let timeout = TimeSpan.FromSeconds 5. // Timeout applied per request to CosmosDb, including retry attempts
            let context = Cosmos.connect (mode, Equinox.CosmosStore.Discovery.ConnectionString connectionString, database, container) (timeout, retriesOn429Throttling, timeout)
            Store.Context.Cosmos (context, cache)
//#endif
//#if dynamo
        | Context.Dynamo (region, table, cache) ->
            let cache = Equinox.Cache("Dynamo", sizeMb = cache)
            let retries = 1 // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            let timeout = TimeSpan.FromSeconds 5. // Timeout applied per request, including retry attempts
            let context = Dynamo.connect (region, table) (timeout, retries)
            Store.Context.Dynamo (context, cache)
//#endif

/// Dependency Injection wiring for services using Equinox
module Services =

    /// Registers the Equinox Store, Stream Resolver, Service Builder and the Service
    let register (services: IServiceCollection, storeCfg) =
        let store = Store.connect storeCfg
//#if todos
        services.AddSingleton(Todo.Factory.create store) |> ignore
//#endif
//#if aggregate
        services.AddSingleton(Aggregate.Factory.create store) |> ignore
//#else
        //services.AddSingleton(Thing.Config.create store) |> ignore
//#endif

/// Defines the Hosting configuration, including registration of the store and backend services
type Startup() =

    // This method gets called by the runtime. Use this method to add services to the container.
    member _.ConfigureServices(services: IServiceCollection): unit =
        services
            .AddMvc()
            .AddJsonOptions(fun options ->
                FsCodec.SystemTextJson.Options.Default.Converters
                |> Seq.iter options.JsonSerializerOptions.Converters.Add
            ) |> ignore

//#if (cosmos || eventStore || dynamo)
        // This is the allocation limit passed internally to a System.Caching.MemoryCache instance
        // The primary objects held in the cache are the Folded State of Event-sourced aggregates
        // see https://docs.microsoft.com/en-us/dotnet/framework/performance/caching-in-net-framework-applications for more information
        let cacheMb = 50

//#endif
//#if eventStore
        // EVENTSTORE: See https://github.com/jet/equinox/blob/master/docker-compose.yml for the associated docker-compose configuration
        
        let storeConfig = Store.Context.Esdb ("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false", cacheMb)

//#endif
//#if cosmos
        // AZURE COSMOSDB: Events are stored as items in a CosmosDb Container
        // Provisioning Steps:
        // 1) Set the 3x environment variables EQUINOX_COSMOS_CONNECTION, EQUINOX_COSMOS_DATABASE, EQUINOX_COSMOS_CONTAINER
        // 2) Provision a container using the following command sequence:
        //     dotnet tool install -g Equinox.Tool
        //     eqx init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
        let storeConfig =
            let connectionVar, databaseVar, containerVar = "EQUINOX_COSMOS_CONNECTION", "EQUINOX_COSMOS_DATABASE", "EQUINOX_COSMOS_CONTAINER"
            let read key = Environment.GetEnvironmentVariable key |> Option.ofObj
            match read connectionVar, read databaseVar, read containerVar with
            | Some connection, Some database, Some container ->
                let connMode = Microsoft.Azure.Cosmos.ConnectionMode.Direct // Best perf - select one of the others iff using .NETCore on linux or encounter firewall issues
                Store.Context.Cosmos (connMode, connection, database, container, cacheMb)
//#if cosmosSimulator
            | None, Some database, Some container ->
                // alternately, you can feed in this connection string in as a parameter externally and remove this special casing
                let wellKnownConnectionStringForCosmosDbSimulator =
                    "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"
                Store.Context.Cosmos (Microsoft.Azure.Cosmos.ConnectionMode.Direct, wellKnownConnectionStringForCosmosDbSimulator, database, container, cacheMb)
//#endif
            | _ ->
                failwithf "Event Storage subsystem requires the following Environment Variables to be specified: %s, %s, %s" connectionVar databaseVar containerVar

//#endif
//#if dynamo
        let storeConfig =
            let regionVar, tableVar = "EQUINOX_DYNAMO_REGION", "EQUINOX_DYNAMO_TABLE"
            let read key = Environment.GetEnvironmentVariable key |> Option.ofObj
            match read regionVar, read tableVar with
            | Some region, Some table ->
                Store.Context.Dynamo (region, table, cacheMb)
            | _ ->
                failwithf "Event Storage subsystem requires the following Environment Variables to be specified: %s, %s" regionVar tableVar

//#endif
#if (memoryStore && !cosmos && !dynamo && !eventStore)
        let storeConfig = Store.Context.Memory

#endif
//#if (!memoryStore && !cosmos && !dynamo && !eventStore)
        //let storeConfig = Store.Context.Memory

//#endif
        Services.register(services, storeConfig)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member _.Configure(app: IApplicationBuilder, env: IHostEnvironment): unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseRouting()
            .UseSerilogRequestLogging() // see https://nblumhardt.com/2019/10/serilog-in-aspnetcore-3/
#if todos
            // NB Jet does now own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing use in your environment before using it._
            .UseCors(fun x -> x.WithOrigins([|"https://www.todobackend.com"|]).AllowAnyHeader().AllowAnyMethod() |> ignore)
#endif
            .UseEndpoints(fun endpoints ->
                endpoints.MapMetrics() |> ignore // Host /metrics for Prometheus
                endpoints.MapControllers() |> ignore)
            |> ignore
