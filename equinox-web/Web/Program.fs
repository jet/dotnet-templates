module TodoBackendTemplate.Web.Program

open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Prometheus
open Serilog

let [<Literal>] AppName = "TodoBackendTemplate"

[<EntryPoint>]
let main argv =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
#if cosmos
            .WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(["app", AppName]))
#endif
#if dynamo
            .WriteTo.Sink(Equinox.DynamoStore.Prometheus.LogSink(["app", AppName]))
#endif
            .Enrich.WithProperty("app", AppName)
            .Enrich.FromLogContext()
            .WriteTo.Console()
            .CreateLogger()
    try try
        let builder = WebApplication.CreateBuilder argv
        builder.Host.UseSerilog() |> ignore

        builder.Services
            .AddMvc()
            .AddJsonOptions(fun options ->
                FsCodec.SystemTextJson.Options.Default.Converters
                |> Seq.iter options.JsonSerializerOptions.Converters.Add
            ) |> ignore

//#if todos
        builder.Services.AddCors() |> ignore
//#endif

//#if (cosmos || eventStore || dynamo)
        // This is the allocation limit passed internally to a System.Caching.MemoryCache instance
        // The primary objects held in the cache are the Folded State of Event-sourced aggregates
        // see https://docs.microsoft.com/en-us/dotnet/framework/performance/caching-in-net-framework-applications for more information
        let cacheMb = 50

//#endif
#if eventStore
        // EVENTSTORE: See https://github.com/jet/equinox/blob/master/docker-compose.yml for the associated docker-compose configuration

        let storage = Storage.Config.Esdb ("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false", cacheMb)

#endif
#if cosmos
        // AZURE COSMOSDB: Events are stored as items in a CosmosDb Container
        // Provisioning Steps:
        // 1) Set the 3x environment variables EQUINOX_COSMOS_CONNECTION, EQUINOX_COSMOS_DATABASE, EQUINOX_COSMOS_CONTAINER
        // 2) Provision a container using the following command sequence:
        //     dotnet tool install -g Equinox.Tool
        //     eqx init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
        let storage =
            let connectionVar, databaseVar, containerVar = "EQUINOX_COSMOS_CONNECTION", "EQUINOX_COSMOS_DATABASE", "EQUINOX_COSMOS_CONTAINER"
            let read key = Environment.GetEnvironmentVariable key |> Option.ofObj
            match read connectionVar, read databaseVar, read containerVar with
            | Some connection, Some database, Some container ->
                let connMode = Microsoft.Azure.Cosmos.ConnectionMode.Direct // Best perf - select one of the others iff using .NETCore on linux or encounter firewall issues
                Store.Config.Cosmos (connMode, connection, database, container, cacheMb)
//#if cosmosSimulator
            | None, Some database, Some container ->
                // alternately, you can feed in this connection string in as a parameter externally and remove this special casing
                let wellKnownConnectionStringForCosmosDbSimulator =
                    "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"
                Storage.Config.Cosmos (Microsoft.Azure.Cosmos.ConnectionMode.Direct, wellKnownConnectionStringForCosmosDbSimulator, database, container, cacheMb)
//#endif
            | _ -> failwith $"Event Storage subsystem requires the following Environment Variables to be specified: %s{connectionVar}, %s{databaseVar}, %s{containerVar}"

#endif
#if dynamo
        let storage =
            let regionVar, tableVar = "EQUINOX_DYNAMO_REGION", "EQUINOX_DYNAMO_TABLE"
            let read key = Environment.GetEnvironmentVariable key |> Option.ofObj
            match read regionVar, read tableVar with
            | Some region, Some table ->
                Storage.Config.Dynamo (region, table, cacheMb)
            | _ -> failwith $"Event Storage subsystem requires the following Environment Variables to be specified: %s{regionVar}, %s{tableVar}"

#endif
#if (memoryStore && !cosmos && !dynamo && !eventStore)
        let storage = Storage.Config.Memory

#endif
//#if (!memoryStore && !cosmos && !dynamo && !eventStore)
        let storage = Storage.Config.Memory

//#endif
        Services.register (builder.Services, storage)

        let app = builder.Build()

        if app.Environment.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection() |> ignore
        app.UseSerilogRequestLogging() |> ignore // see https://nblumhardt.com/2019/10/serilog-in-aspnetcore-3/
//#if todos
        // NB Jet does not own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing to use in your environment before using it.
        app.UseCors(fun x -> x.WithOrigins([|"https://www.todobackend.com"|]).AllowAnyHeader().AllowAnyMethod() |> ignore) |> ignore
//#endif
        app.MapControllers() |> ignore
        app.MapMetrics() |> ignore // Host /metrics for Prometheus

        app.Run()
        0
        with e -> Log.Fatal(e, "Application Startup failed"); 1
    finally Log.CloseAndFlush()
