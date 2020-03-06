module TodoBackendTemplate.Web.Program

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Hosting
open Serilog

let initLogging () =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
            .Enrich.FromLogContext()
            .WriteTo.Console()
            .CreateLogger()

let createWebHostBuilder args : IWebHostBuilder =
    WebHost
        .CreateDefaultBuilder(args)
        .UseSerilog()
        .UseStartup<Startup>()

[<EntryPoint>]
let main argv =
    try initLogging ()
        createWebHostBuilder(argv).Build().Run()
        0
    with e ->
        eprintfn "%s" e.Message
        1
