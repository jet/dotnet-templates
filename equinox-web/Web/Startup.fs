namespace Web

open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Serilog
open System
open TodoBackend

/// Equinox store bindings
module Storage =
    /// Specifies the store to be used, together with any relevant custom parameters
    type Config =
        | Mem
        | ES of host: string * username: string * password: string * cacheGb: int

    /// Holds an initialized/customized/configured of the store as defined by the `Config`
    type Instance =
        | MemoryStore of Equinox.MemoryStore.VolatileStore
        | EventStore of gateway: Equinox.EventStore.GesGateway * cache: Equinox.EventStore.Caching.Cache

    /// MemoryStore 'wiring', uses Equinox.MemoryStore nuget package
    module private Memory =
        open Equinox.MemoryStore
        let connect () =
            VolatileStore()

    /// EventStore wiring, uses Equinox.EventStore nuget package
    module private ES =
        open Equinox.EventStore
        let mkCache cacheGb =
            let bytes = cacheGb*1024*1024
            Caching.Cache ("ES", bytes)
        let connect host username password=
            let log = Logger.SerilogNormal (Log.ForContext<Instance>())
            let c = GesConnector(username,password,reqTimeout=TimeSpan.FromSeconds 5., reqRetries=1, log=log)
            let conn = c.Establish ("Twin", Discovery.GossipDns host, ConnectionStrategy.ClusterTwinPreferSlaveReads) |> Async.RunSynchronously
            GesGateway(conn, GesBatchingPolicy(maxBatchSize=500))

    /// Creates and/or connects to a specific store as dictated by the specified config
    let connect : Config -> Instance = function
        | Mem ->
            let store = Memory.connect()
            Instance.MemoryStore store
        | ES (host, user, pass, cache) ->
            let cache = ES.mkCache cache
            let conn = ES.connect host user pass
            Instance.EventStore (conn, cache)

/// Dependency Injection wiring for services using Equinox
module Services =
    /// Allows one to hook in any JsonConverters etc
    let serializationSettings = Newtonsoft.Json.JsonSerializerSettings()

    /// Automatically generates a Union Codec based using the scheme described in https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/
    let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

    /// Builds a Stream Resolve function appropriate to the store being used
    type StreamResolver(storage) =
        member __.Resolve
            (   codec : Equinox.UnionCodec.IUnionEncoder<'event,byte[]>,
                fold: ('state -> 'event seq -> 'state),
                initial: 'state,
                snapshot: (('event -> bool) * ('state -> 'event))) =
            match storage with
            | Storage.MemoryStore store ->
                Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve
            | Storage.EventStore (gateway, cache) ->
                let accessStrategy = Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot
                let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
                Equinox.EventStore.GesResolver<'event,'state>(gateway, codec, fold, initial, accessStrategy, cacheStrategy).Resolve

    /// Binds a storage independent Service's Handler's `resolve` function to a given Stream Policy using the StreamResolver
    type ServiceBuilder(resolver: StreamResolver, handlerLog : ILogger) =
         member __.CreateTodosService() =
            let codec = genCodec<Todo.Events.Event>()
            let fold, initial, snapshot = Todo.Folds.fold, Todo.Folds.initial, Todo.Folds.snapshot
            Todo.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot))

    /// F# syntactic sugar for registering services
    type IServiceCollection with
        /// Register a Service as a Singleton, by supplying a function that can build an instance of the type in question
        member services.Register(factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore
    
    /// F# syntactic sugar to resolve a service dependency
    type IServiceProvider with member sp.Resolve<'t>() = sp.GetRequiredService<'t>()

    /// Registers the Equinox Store, Stream Resolver, Service Builder and the Service
    let register (services : IServiceCollection, storeCfg) =
        services.Register(fun _sp -> Storage.connect storeCfg)
        services.Register(fun sp -> StreamResolver(sp.Resolve()))
        services.Register(fun sp -> ServiceBuilder(sp.Resolve(), Log.ForContext<Equinox.Handler<_,_>>()))
        services.Register(fun sp -> sp.Resolve<ServiceBuilder>().CreateTodosService())

/// Defines the Hosting configuration, including registration of the store and backend services
type Startup() =

    // This method gets called by the runtime. Use this method to add services to the container.
    member __.ConfigureServices(services: IServiceCollection) : unit =
        services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore

        let storeConfig = Storage.Mem

        // EVENTSTORE: see https://eventstore.org/
        // Requires a Commercial HA Cluster, which can be simulated by 1) installing the OSS Edition from Choocolatey 2) running it in cluster mode

        //# requires admin privilege
        //cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
        //# run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
        //& $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

        let storeConfig = Storage.ES ("localhost","admin","changeit",2)

        Services.register(services, storeConfig)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member __.Configure(app: IApplicationBuilder, env: IHostingEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseCors(fun x -> x.WithOrigins([|"https://www.todobackend.com"|]).AllowAnyHeader().AllowAnyMethod() |> ignore)
            .UseMvc() |> ignore