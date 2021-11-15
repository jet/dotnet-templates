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
module Storage =
    /// Specifies the store to be used, together with any relevant custom parameters
    [<RequireQualifiedAccess>]
    type Store =
//#if (memoryStore || (!cosmos && !eventStore))
        | Memory
//#endif
//#if eventStore
        | Esdb of host: string * username: string * password: string * cacheMb: int
//#endif
//#if cosmos
        | Cosmos of mode: Microsoft.Azure.Cosmos.ConnectionMode * connectionStringWithUriAndKey: string * database: string * container: string * cacheMb: int
//#endif

//#if (memoryStore || (!cosmos && !eventStore))
    /// MemoryStore 'wiring', uses Equinox.MemoryStore nuget package
    module private Memory =
        open Equinox.MemoryStore
        let connect () =
            VolatileStore()

//#endif
//#if eventStore
    /// EventStore wiring, uses Equinox.EventStore nuget package
    module private ES =
        open Equinox.EventStore
        let connect host username password =
            let log = Logger.SerilogNormal (Log.ForContext<Store>())
            let c = Connector(username, password, reqTimeout=TimeSpan.FromSeconds 5., reqRetries=1, log=log)
            let conn = c.Establish ("Twin", Discovery.GossipDns host, ConnectionStrategy.ClusterTwinPreferSlaveReads) |> Async.RunSynchronously
            EventStoreContext(conn, BatchingPolicy(maxBatchSize=500))

//#endif
//#if cosmos
    /// CosmosDb wiring, uses Equinox.CosmosStore nuget package
    module private Cosmos =
        open Equinox.CosmosStore
        module CosmosStoreContext =

            /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
            let create (storeClient : CosmosStoreClient) =
                let maxEvents = 256
                CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

        let connect (mode, discovery, databaseId, containerId) (operationTimeout, maxRetryForThrottling, maxRetryWait) =
            let c = CosmosStoreConnector(discovery, operationTimeout, maxRetryForThrottling, maxRetryWait, mode)
            CosmosStoreClient.Connect(c.CreateAndInitialize, databaseId, containerId) |> Async.RunSynchronously |> CosmosStoreContext.create

//#endif
    /// Creates and/or connects to a specific store as dictated by the specified config
    let connect = function
//#if (memoryStore || (!cosmos && !eventStore))
        | Store.Memory ->
            let store = Memory.connect()
            Config.Store.Memory store
//#endif
//#if eventStore
        | Store.Esdb (host, user, pass, cache) ->
            let cache = Equinox.Cache("ES", sizeMb=cache)
            let conn = ES.connect host user pass
            Config.Store.Esdb (conn, cache)
//#endif
//#if cosmos
        | Store.Cosmos (mode, connectionString, database, container, cache) ->
            let cache = Equinox.Cache("Cosmos", sizeMb=cache)
            let retriesOn429Throttling = 1 // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            let timeout = TimeSpan.FromSeconds 5. // Timeout applied per request to CosmosDb, including retry attempts
            let context = Cosmos.connect (mode, Equinox.CosmosStore.Discovery.ConnectionString connectionString, database, container) (timeout, retriesOn429Throttling, timeout)
            Config.Store.Cosmos (context, cache)
//#endif

/// Dependency Injection wiring for services using Equinox
module Services =

    /// Registers the Equinox Store, Stream Resolver, Service Builder and the Service
    let register (services : IServiceCollection, storeCfg) =
        let store = Storage.connect storeCfg
//#if todos
        services.AddSingleton(Todo.Config.create store) |> ignore
//#endif
//#if aggregate
        services.AddSingleton(Aggregate.Config.create store) |> ignore
//#else
        //services.AddSingleton(Thing.Config.create store) |> ignore
//#endif

/// Defines the Hosting configuration, including registration of the store and backend services
type Startup() =
    // This method gets called by the runtime. Use this method to add services to the container.
    member _.ConfigureServices(services: IServiceCollection) : unit =
        services
            .AddMvc()
            .SetCompatibilityVersion(CompatibilityVersion.Latest)
            // While FsCodec.SystemTextJson is available and works well, until FsCodec.SystemTextJson has a UnionConverter https://github.com/jet/FsCodec/pull/59, use JSON.NET
            .AddNewtonsoftJson(fun options ->
                FsCodec.NewtonsoftJson.Serdes.DefaultSettings.Converters
                |> Seq.iter options.SerializerSettings.Converters.Add
            )|> ignore

//#if (cosmos || eventStore)
        // This is the allocation limit passed internally to a System.Caching.MemoryCache instance
        // The primary objects held in the cache are the Folded State of Event-sourced aggregates
        // see https://docs.microsoft.com/en-us/dotnet/framework/performance/caching-in-net-framework-applications for more information
        let cacheMb = 50

//#endif
//#if eventStore
        // EVENTSTORE: see https://eventstore.org/
        // Requires a Commercial HA Cluster, which can be simulated by 1) installing the OSS Edition from Chocolatey 2) running it in cluster mode

        //# requires admin privilege
        //cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
        //# run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
        //& $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

        let storeConfig = Storage.Store.Esdb ("localhost", "admin", "changeit", cacheMb)

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
                Storage.Store.Cosmos (connMode, connection, database, container, cacheMb)
//#if cosmosSimulator
            | None, Some database, Some container ->
                // alternately, you can feed in this connection string in as a parameter externally and remove this special casing
                let wellKnownConnectionStringForCosmosDbSimulator =
                    "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"
                Storage.Store.Cosmos (Microsoft.Azure.Cosmos.ConnectionMode.Direct, wellKnownConnectionStringForCosmosDbSimulator, database, container, cacheMb)
//#endif
            | _ ->
                failwithf "Event Storage subsystem requires the following Environment Variables to be specified: %s, %s, %s" connectionVar databaseVar containerVar

//#endif
#if (memoryStore && !cosmos && !eventStore)
        let storeConfig = Storage.Config.Mem

#endif
//#if (!memoryStore && !cosmos && !eventStore)
        //let storeConfig = Storage.Config.Mem

//#endif
        Services.register(services, storeConfig)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member _.Configure(app: IApplicationBuilder, env: IHostEnvironment) : unit =
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
