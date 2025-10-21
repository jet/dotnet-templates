[<AutoOpen>]
module PrunerTemplate.Infrastructure

open Serilog
open System

module Store =

    module Metrics =
        
        let [<Literal>] PropertyTag = "isMetric"
        let log = Log.ForContext(PropertyTag, true)
        /// Allow logging to filter out emission of log messages whose information is also surfaced as metrics
        let logEventIsMetric e = Filters.Matching.WithProperty(PropertyTag).Invoke e

module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Log =

    /// The Propulsion.Streams.Prometheus LogSink uses this well-known property to identify consumer group associated with the Scheduler
    let forGroup group = Log.ForContext("group", group)

module private CosmosStoreConnector =

    let private logContainer (role: string) (databaseId: string) (containerId: string) =
        Log.Information("CosmosDB {role} Database {database} Container {container}", role, databaseId, containerId)
    let getSource (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
        logContainer "Source" databaseId containerId
        client.GetDatabase(databaseId).GetContainer(containerId)
    let getLeases (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId auxContainerId =
        logContainer "Leases" databaseId auxContainerId
        client.GetDatabase(databaseId).GetContainer(auxContainerId)
    let getSourceAndLeases client databaseId containerId auxContainerId =
        getSource client databaseId containerId, getLeases client databaseId auxContainerId

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role: string, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents, ?tipMaxJsonLength) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents, ?tipMaxJsonLength = tipMaxJsonLength)
        c.LogConfiguration(role, databaseId, containerId)
        c

type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(role, databaseId: string, containers: string[]) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDB {role} {mode} {endpointUri} {db} {containers} timeout {timeout}s Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        role, o.ConnectionMode, x.Endpoint, databaseId, containers, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
    member private x.CreateAndInitialize(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.CreateAndInitialize(databaseId, containers)
    member private x.Connect(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.Connect(databaseId, containers)
    member x.ConnectContext(role, databaseId, containerId, tipMaxEvents, ?auxContainerId) = async {
        let! client = x.Connect(role, databaseId, [| yield containerId; yield! Option.toList auxContainerId |])
        return client.CreateContext(role, databaseId, containerId, tipMaxEvents) }
    member x.ConnectFeed(databaseId, containerId, auxContainerId) = async {
        let! cosmosClient = x.CreateAndInitialize("Source", databaseId, [| containerId; auxContainerId |])
        return CosmosStoreConnector.getSourceAndLeases cosmosClient databaseId containerId auxContainerId }
    member x.ConnectFeed(databaseId, containerId, auxContainer) = async {
        let! cosmosClient = x.CreateAndInitialize("Source", databaseId, [| containerId |])
        return CosmosStoreConnector.getSource cosmosClient databaseId containerId, auxContainer }
    member x.LeasesContainer(databaseId, auxContainerId) =
        let client = x.CreateUninitialized()
        CosmosStoreConnector.getLeases client databaseId auxContainerId

/// Equinox and Propulsion provide metrics as properties in log emissions
/// These helpers wire those to pass through virtual Log Sinks that expose them as Prometheus metrics.
module Sinks =

    let tags appName = ["app", appName]

    let equinoxMetricsOnly tags (l: LoggerConfiguration) =
        l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
         .WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(tags))

    let equinoxAndPropulsionConsumerMetrics tags (l: LoggerConfiguration) =
        l |> equinoxMetricsOnly tags
          |> fun l -> l.WriteTo.Sink(Propulsion.Prometheus.LogSink(tags))

    let equinoxAndPropulsionCosmosConsumerMetrics tags (l: LoggerConfiguration) =
        l |> equinoxAndPropulsionConsumerMetrics tags
          |> fun l -> l.WriteTo.Sink(Propulsion.CosmosStore.Prometheus.LogSink(tags))

    let console verbose (configuration: LoggerConfiguration) =
        let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
        let t = if verbose then t else t.Replace("{Properties:j}", "")
        configuration.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c

    [<System.Runtime.CompilerServices.Extension>]
    static member private Sinks(configuration: LoggerConfiguration, configureMetricsSinks, configureConsoleSink, ?isMetric) =
        let configure (a: Configuration.LoggerSinkConfiguration): unit =
            a.Logger(configureMetricsSinks >> ignore) |> ignore // unconditionally feed all log events to the metrics sinks
            a.Logger(fun l -> // but filter what gets emitted to the console sink
                let l = match isMetric with None -> l | Some predicate -> l.Filter.ByExcluding(Func<Serilog.Events.LogEvent, bool> predicate)
                configureConsoleSink l |> ignore)
            |> ignore
        configuration.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, appName, verbose, cfpVerbose) =
        configuration.Configure(verbose)
        |> fun c -> let ingesterLevel = if cfpVerbose then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Sinks.Factory>.FullName, ingesterLevel)
        |> fun c -> let generalLevel = if verbose then Events.LogEventLevel.Information else Events.LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.CosmosStore.Internal.Writer.Result>.FullName, generalLevel)
        |> fun c -> let isWriterB = Filters.Matching.FromSource<Propulsion.CosmosStore.Internal.Writer.Result>().Invoke
                    let metricFilter = if cfpVerbose then None else Some (fun x -> Store.Metrics.logEventIsMetric x || isWriterB x)
                    c.Sinks(Sinks.equinoxAndPropulsionCosmosConsumerMetrics (Sinks.tags appName), Sinks.console verbose, ?isMetric = metricFilter)
