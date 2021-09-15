namespace ReactorTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

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

    let defaultForMetrics = Log.ForContext<Equinox.CosmosStore.CosmosStoreContext>()
    let isForMetrics = Filters.Matching.FromSource<Equinox.CosmosStore.CosmosStoreContext>().Invoke

module Equinox =

    let createDecider stream =
        Equinox.Decider(Log.defaultForMetrics, stream, maxAttempts = 3)

module Guid =

    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value : ClientId) : string = Guid.toStringN %value
    let parse (value : string) : ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

[<System.Runtime.CompilerServices.Extension>]
type LoggerConfigurationExtensions() =

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        let cfpl = if verbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
        // TODO figure out what CFP v3 requires
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c

    [<System.Runtime.CompilerServices.Extension>]
    static member AsHost(configuration : LoggerConfiguration, appName) =
        let customTags = ["app",appName]
        configuration.WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(customTags))

    [<System.Runtime.CompilerServices.Extension>]
    static member AsPropulsionConsumer(configuration : LoggerConfiguration, appName, group) =
        let customTags = ["app", appName]
        configuration.AsHost(appName)
        |> fun c -> c.WriteTo.Sink(Propulsion.Prometheus.LogSink(customTags, group))

    [<System.Runtime.CompilerServices.Extension>]
    static member AsPropulsionCosmosConsumer(configuration : LoggerConfiguration, appName, group, ?changeFeedProcessorVerbose) =
        let customTags = ["app", appName]
        configuration.ConfigureChangeFeedProcessorLogging((changeFeedProcessorVerbose = Some true))
        |> fun c -> c.WriteTo.Sink(Propulsion.CosmosStore.Prometheus.LogSink(customTags))
        |> fun c -> c.AsPropulsionConsumer(appName, group)

    [<System.Runtime.CompilerServices.Extension>]
    static member ToConsole(configuration : LoggerConfiguration) =
        let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {NewLine}{Exception}"
        configuration.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)

    [<System.Runtime.CompilerServices.Extension>]
    static member Default(configuration : LoggerConfiguration, storeVerbose) =
        if storeVerbose then configuration else configuration.Filter.ByExcluding(fun x -> Log.isForMetrics x)
        |> fun c -> c.ToConsole().CreateLogger()

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

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

        /// Use sparingly; in general one wants to use CreateAndInitialize to avoid slow first requests
        member x.CreateUninitialized(databaseId, containerId) =
            x.CreateUninitialized().GetDatabase(databaseId).GetContainer(containerId)

        /// Connect a CosmosStoreClient, including warming up
        member x.ConnectStore(connectionName, databaseId, containerId) =
            x.LogConfiguration(connectionName, databaseId, containerId)
            Equinox.CosmosStore.CosmosStoreClient.Connect(x.CreateAndInitialize, databaseId, containerId)

        /// Creates a CosmosClient suitable for running a CFP via CosmosStoreSource
        member x.ConnectMonitored(databaseId, containerId, ?connectionName) =
            x.LogConfiguration(defaultArg connectionName "Source", databaseId, containerId)
            x.CreateUninitialized(databaseId, containerId)

        /// Connects to a Store as both a ChangeFeedProcessor Monitored Container and a CosmosStoreClient
        member x.ConnectStoreAndMonitored(databaseId, containerId) =
            let monitored = x.ConnectMonitored(databaseId, containerId, "Main")
            let storeClient = Equinox.CosmosStore.CosmosStoreClient(monitored.Database.Client, databaseId, containerId)
            storeClient, monitored
