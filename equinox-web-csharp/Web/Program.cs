using Prometheus;
using Serilog;
using Serilog.Events;
using TodoBackendTemplate;
using TodoBackendTemplate.Web;

const string AppName = "TodoBackendTemplate";

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
//#if cosmos
    .WriteTo.Sink(new Equinox.CosmosStore.Prometheus.LogSink([Tuple.Create("app", AppName)]))
//#endif
    .Enrich.WithProperty("app", AppName)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog();

    builder.Services
        .AddMvc()
        .AddJsonOptions(o =>
        {
            foreach (var c in FsCodec.SystemTextJson.Options.Default.Converters)
                o.JsonSerializerOptions.Converters.Add(c);
        });
//#if todos
    builder.Services.AddCors();
//#endif

    var equinoxContext = ConfigureStore();
    builder.Services.AddSingleton(_ => equinoxContext);
    builder.Services.AddSingleton(sp => new ServiceBuilder(equinoxContext, Serilog.Log.ForContext<EquinoxContext>()));
//#if todos
    builder.Services.AddSingleton(sp => sp.GetRequiredService<ServiceBuilder>().CreateTodoService());
//#endif
#if aggregate
    builder.Services.AddSingleton(sp => sp.GetRequiredService<ServiceBuilder>().CreateAggregateService());
#endif
#if (!aggregate && !todos)
    //builder.Services.AddSingleton(sp => sp.GetRequiredService<ServiceBuilder>().CreateThingService());
#endif

    var app = builder.Build();

    if (app.Environment.IsDevelopment())
        app.UseDeveloperExceptionPage();
    else
        app.UseHsts();

    app.UseHttpsRedirection()
       .UseSerilogRequestLogging() // see https://nblumhardt.com/2019/10/serilog-in-aspnetcore-3/
#if todos
       // NB Jet does not own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing to use in your environment before using it.
       .UseCors(x => x.WithOrigins("https://www.todobackend.com").AllowAnyHeader().AllowAnyMethod())
#endif
       ;

// add controllers from this assembly
    app.MapControllers();
    app.MapMetrics(); // Host /metrics for Prometheus

    foreach (var ctx in app.Services.GetServices<EquinoxContext>())
        await ctx.Connect();

    await app.RunAsync();
    return 0;
}
catch (Exception e)
{
    Console.Error.WriteLine(e.Message);
    return 1;
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

    var esConfig = new EventStoreConfig("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false", cacheMb);
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
#if (!cosmos && !dynamo && !eventStore)
    return new MemoryStoreContext(new Equinox.MemoryStore.VolatileStore<ReadOnlyMemory<byte>>());
#endif
#if (!memoryStore && !cosmos && !dynamo && !eventStore)
    //return new MemoryStoreContext(new Equinox.MemoryStore.VolatileStore());
#endif
}