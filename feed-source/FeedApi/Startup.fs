namespace FeedApiTemplate

open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Serilog

type Startup private () =
    new (configuration : IConfiguration) as this =
        Startup() then
        this.Configuration <- configuration

    member this.ConfigureServices(services : IServiceCollection) =
        services.AddControllers() |> ignore

    member this.Configure(app : IApplicationBuilder, env : IWebHostEnvironment) =
        if env.IsDevelopment() then
            app.UseDeveloperExceptionPage() |> ignore

        app.UseHttpsRedirection() |> ignore
        app.UseRouting() |> ignore

        app.UseAuthorization() |> ignore

        app.UseEndpoints(fun endpoints ->
            endpoints.MapControllers() |> ignore
            ) |> ignore

    member val Configuration : IConfiguration = null with get, set

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(c : LoggerConfiguration) =
        c
            .MinimumLevel.Debug()
            .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
            .Enrich.FromLogContext()
            .WriteTo.Console()

module Hosting =

    let createHostBuilder () : IHostBuilder =
        Host.CreateDefaultBuilder()
            .UseSerilog()
            .ConfigureWebHostDefaults(fun webBuilder ->
                webBuilder.UseStartup<Startup>() |> ignore
            )
