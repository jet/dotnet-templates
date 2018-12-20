namespace TodoBackend.Web

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Hosting
open Serilog

module Program =
    let createWebHostBuilder args : IWebHostBuilder =
        WebHost
            .CreateDefaultBuilder(args)
            .UseStartup<Startup>()
            .UseSerilog()

    [<EntryPoint>]
    let main argv =
        try
            Log.Logger <-
                LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Console()
                    .CreateLogger()
                :> ILogger
            createWebHostBuilder(argv).Build().Run()
            0
        with e ->
            eprintfn "%s" e.Message
            1