using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing.Matching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Prometheus;
using Serilog;

namespace TodoBackendTemplate.Web
{
    /// <summary>Defines the Hosting configuration, including registration of the store and backend services</summary>
    class Startup
    {
        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostEnvironment env)
        {
            if (env.IsDevelopment())
                app.UseDeveloperExceptionPage();
            else
                app.UseHsts();

            app.UseHttpsRedirection()
                .UseRouting()
                .UseSerilogRequestLogging() // see https://nblumhardt.com/2019/10/serilog-in-aspnetcore-3/

#if todos
                // NB Jet does now own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing use in your environment before using it._
                .UseCors(x => x.WithOrigins("https://www.todobackend.com").AllowAnyHeader().AllowAnyMethod())
#endif
                .UseEndpoints(endpoints =>
                {
                    endpoints.MapControllers();
                    endpoints.MapMetrics(); // Host /metrics for Prometheus
                });
        }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)

        {
            services
                .AddMvc()
                .SetCompatibilityVersion(CompatibilityVersion.Latest)
                .AddNewtonsoftJson();
            var equinoxContext = ConfigureStore();
            ConfigureServices(services, equinoxContext);
        }

        static void ConfigureServices(IServiceCollection services, EquinoxContext context)
        {
            services.AddSingleton(_ => context);
            services.AddSingleton(sp => new ServiceBuilder(context, Serilog.Log.ForContext<EquinoxContext>()));
#if todos
            services.AddSingleton(sp => sp.GetRequiredService<ServiceBuilder>().CreateTodoService());
#endif
#if aggregate
            services.AddSingleton(sp => sp.GetRequiredService<ServiceBuilder>().CreateAggregateService());
#endif
#if (!aggregate && !todos)
            //services.Register(fun sp -> sp.Resolve<ServiceBuilder>().CreateThingService())
#endif
        }

        static EquinoxContext ConfigureStore()
        {
#if (cosmos || eventStore)
            // This is the allocation limit passed internally to a System.Caching.MemoryCache instance
            // The primary objects held in the cache are the Folded State of Event-sourced aggregates
            // see https://docs.microsoft.com/en-us/dotnet/framework/performance/caching-in-net-framework-applications for more information
            var cacheMb = 50;

#endif
#if eventStore
            // EVENTSTORE: see https://eventstore.org/
            // Requires a Commercial HA Cluster, which can be simulated by 1) installing the OSS Edition from Chocolatey 2) running it in cluster mode

            // # requires admin privilege
            // cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
            // # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
            // & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

            var esConfig = new EventStoreConfig("localhost", "admin", "changeit", cacheMb);
            return new EventStoreContext(esConfig);
#endif
#if cosmos
            // AZURE COSMOSDB: Events are stored in an Azure CosmosDb Account (using the SQL API)
            // Provisioning Steps:
            // 1) Set the 3x environment variables EQUINOX_COSMOS_CONNECTION, EQUINOX_COSMOS_DATABASE, EQUINOX_COSMOS_CONTAINER
            // 2) Provision a container using the following command sequence:
            //     dotnet tool install -g Equinox.Tool
            //     eqx init -ru 400 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
            const string connVar = "EQUINOX_COSMOS_CONNECTION";
            var conn = Environment.GetEnvironmentVariable(connVar);
            const string dbVar = "EQUINOX_COSMOS_DATABASE";
            var db = Environment.GetEnvironmentVariable(dbVar);
            const string containerVar = "EQUINOX_COSMOS_CONTAINER";
            var container = Environment.GetEnvironmentVariable(containerVar);
            if (conn == null || db == null || container == null)
                throw new Exception(
                    $"Event Storage subsystem requires the following Environment Variables to be specified: {connVar} {dbVar}, {containerVar}");
            var connMode = Microsoft.Azure.Cosmos.ConnectionMode.Direct;
            var config = new CosmosConfig(connMode, conn, db, container, cacheMb);
            return new CosmosContext(config);
#endif
#if (!cosmos && !eventStore)
            return new MemoryStoreContext(new Equinox.MemoryStore.VolatileStore<byte[]>());
#endif
#if (!memoryStore && !cosmos && !eventStore)
            //return new MemoryStoreContext(new Equinox.MemoryStore.VolatileStore());
#endif
        }
    }

    /// Binds a storage independent Service's Handler's `resolve` function to a given Stream Policy using the StreamResolver
    internal class ServiceBuilder
    {
        readonly EquinoxContext _context;
        readonly ILogger _handlerLog;

        public ServiceBuilder(EquinoxContext context, ILogger handlerLog)
        {
            _context = context;
            _handlerLog = handlerLog;
        }

#if todos
        public Todo.Service CreateTodoService() =>
            new Todo.Service(
                _handlerLog,
                _context.Resolve(
                    EquinoxCodec.Create(Todo.Event.Encode, Todo.Event.TryDecode),
                    Todo.State.Fold,
                    Todo.State.Initial,
                    Todo.State.IsOrigin,
                    Todo.State.Snapshot));
#endif
#if aggregate
        public Aggregate.Service CreateAggregateService() =>
            new Aggregate.Service(
                _handlerLog,
                _context.Resolve(
                    EquinoxCodec.Create(Aggregate.Event.Encode, Aggregate.Event.TryDecode),
                    Aggregate.State.Fold,
                    Aggregate.State.Initial,
                    Aggregate.State.IsOrigin,
                    Aggregate.State.Snapshot));
#endif
#if (!aggregate && !todos)
//        public Thing.Service CreateThingService() =>
//            Thing.Service(
//                _handlerLog,
//                _context.Resolve(
//                    EquinoxCodec.Create<Thing.Events.Event>(), // Requires Union following IUnionContract pattern, see https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/
//                    Thing.Fold.Fold,
//                    Thing.Fold.Initial,
//                    Thing.Fold.IsOrigin,
//                    Thing.Fold.Snapshot));
#endif
    }
}
