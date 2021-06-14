namespace ConsumerTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open Serilog
open System.Runtime.CompilerServices

module EnvVar =

    let tryGet varName : string option = System.Environment.GetEnvironmentVariable varName |> Option.ofObj

module EventCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IEventCodec<_, _, _>) streamName (x : FsCodec.ITimelineEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                Log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, streamName)
            None
        | x -> x

module Log =

    let forMetrics () =
        Serilog.Log.ForContext("isMetric", true)

module Equinox =

    let createDecider stream =
        Equinox.Decider(Log.forMetrics (), stream, maxAttempts = 3)

[<AutoOpen>]
module ConnectorExtensions =

    type Equinox.CosmosStore.CosmosStoreConnector with

        member private x.LogConfiguration(connectionName, databaseId, containerId) =
            let o = x.Options
            let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
            Log.Information("CosmosDb {name} {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                            connectionName, o.ConnectionMode, x.Endpoint, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
            Log.Information("CosmosDb {name} Database {database} Container {container}",
                            connectionName, databaseId, containerId)

        /// Connect a CosmosStoreClient, including warming up
        member x.ConnectStore(connectionName, databaseId, containerId) =
            x.LogConfiguration(connectionName, databaseId, containerId)
            Equinox.CosmosStore.CosmosStoreClient.Connect(x.CreateAndInitialize, databaseId, containerId)
        
/// SkuId strongly typed id; represented internally as a string
type SkuId = string<skuId>
and [<Measure>] skuId
module SkuId =
    let toString (value : SkuId) : string = % value
    let parse (value : string) : SkuId = let raw = value in % raw
    let (|Parse|) = parse

[<Extension>]
type Logging() =

    [<Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                    c.WriteTo.Console(theme=theme, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {Properties}{NewLine}{Exception}")
