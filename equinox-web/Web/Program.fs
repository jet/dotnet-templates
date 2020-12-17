module TodoBackendTemplate.Web.Program

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Hosting
open Serilog

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(c : LoggerConfiguration, appName) =
        c
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
#if cosmos
            .WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(appName))
#endif
            .Enrich.FromLogContext()
            .WriteTo.Console()

let createWebHostBuilder args : IWebHostBuilder =
    WebHost
        .CreateDefaultBuilder(args)
        .UseSerilog()
        .UseStartup<Startup>()

let [<Literal>] AppName = "TodoApp"

[<EntryPoint>]
let main argv =
    try Log.Logger <- LoggerConfiguration().Configure(AppName).CreateLogger()
        createWebHostBuilder(argv).Build().Run()
        0
    with e ->
        eprintfn "%s" e.Message
        1
