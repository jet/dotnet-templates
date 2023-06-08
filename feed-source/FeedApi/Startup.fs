namespace FeedSourceTemplate

open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Prometheus
open Serilog

type Startup() =

    member this.ConfigureServices(services: IServiceCollection) =
        services.AddControllers() |> ignore

    member this.Configure(app: IApplicationBuilder, env: IWebHostEnvironment) =
        if env.IsDevelopment() then
            app.UseDeveloperExceptionPage() |> ignore

        app.UseHttpsRedirection() |> ignore
        app.UseRouting() |> ignore

        app.UseAuthorization() |> ignore

        app.UseEndpoints(fun endpoints ->
            endpoints.MapControllers() |> ignore
            endpoints.MapMetrics() |> ignore // Host /metrics for Prometheus
            ) |> ignore

module Hosting =

    let createHostBuilder (): IHostBuilder =
        Host.CreateDefaultBuilder()
            .UseSerilog()
            .ConfigureWebHostDefaults(fun webBuilder ->
                webBuilder.UseStartup<Startup>() |> ignore
            )
