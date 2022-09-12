[<AutoOpen>]
module ConsumerTemplate.Infrastructure

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open Serilog
open System

module Guid =

    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value : ClientId) : string = Guid.toStringN %value
    let parse (value : string) : ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module EventCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IEventCodec<_, _, _>) streamName (x : FsCodec.ITimelineEvent<Propulsion.Streams.Default.EventBody>) =
        match codec.TryDecode x with
        | ValueNone ->
            if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                Log.ForContext("event", System.Text.Encoding.UTF8.GetString(let d = x.Data in d.Span), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, streamName)
            ValueNone
        | x -> x

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

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                    c.WriteTo.Console(theme=theme, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {Properties}{NewLine}{Exception}")
