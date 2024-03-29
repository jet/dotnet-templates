[<AutoOpen>] 
module Infrastructure

open Serilog
open System

module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Log =

    /// Allow logging to filter out emission of log messages whose information is also surfaced as metrics
    let isStoreMetrics e = Filters.Matching.WithProperty("isMetric").Invoke e

/// Equinox and Propulsion provide metrics as properties in log emissions
/// These helpers wire those to pass through virtual Log Sinks that expose them as Prometheus metrics.
module Sinks =

    let tags appName = ["app", appName]

    let equinoxMetricsOnly tags (l: LoggerConfiguration) =
        l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
         .WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(tags))

    let equinoxAndPropulsionConsumerMetrics tags group (l: LoggerConfiguration) =
        l |> equinoxMetricsOnly tags
          |> fun l -> l.WriteTo.Sink(Propulsion.Prometheus.LogSink(tags, group))

    let equinoxAndPropulsionCosmosConsumerMetrics tags group (l: LoggerConfiguration) =
        l |> equinoxAndPropulsionConsumerMetrics tags group
          |> fun l -> l.WriteTo.Sink(Propulsion.CosmosStore.Prometheus.LogSink(tags))

    let console (configuration: LoggerConfiguration) =
        let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
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
    static member Sinks(configuration: LoggerConfiguration, configureMetricsSinks, verboseStore) =
        configuration.Sinks(configureMetricsSinks, Sinks.console, ?isMetric = if verboseStore then None else Some Log.isStoreMetrics)

module CosmosStoreConnector =

    let private get (role: string) (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
        Log.Information("CosmosDB {role} Database {database} Container {container}", role, databaseId, containerId)
        client.GetDatabase(databaseId).GetContainer(containerId)
    let getSource = get "Source"
    let getLeases = get "Leases"
    let getSourceAndLeases client databaseId containerId auxContainerId =
        getSource client databaseId containerId, getLeases client databaseId auxContainerId

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role: string, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents, ?queryMaxItems, ?tipMaxJsonLength, ?skipLog) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents, ?queryMaxItems = queryMaxItems, ?tipMaxJsonLength = tipMaxJsonLength)
        if skipLog = Some true then () else c.LogConfiguration(role, databaseId, containerId)
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
    member private x.Connect(role, databaseId, containerId, viewsContainerId, ?auxContainerId, ?logSnapshotConfig) = async {
        let! cosmosClient = x.CreateAndInitialize(role, databaseId, [| yield containerId; yield viewsContainerId; yield! Option.toList auxContainerId |])
        let client = Equinox.CosmosStore.CosmosStoreClient(cosmosClient)
        let contexts =
            client.CreateContext(role, databaseId, containerId, tipMaxEvents = 256, queryMaxItems = 500),
            client.CreateContext(role, databaseId, viewsContainerId, tipMaxEvents = 256, queryMaxItems = 500),
            // NOTE the tip limits for this connection are set to be effectively infinite in order to ensure that writes never trigger calving from the tip
            client.CreateContext("snapshotUpdater", databaseId, containerId, tipMaxEvents = 1024, tipMaxJsonLength = 1024 * 1024,
                                 skipLog = not (logSnapshotConfig = Some true))
        return cosmosClient, contexts }
    member x.ConnectWithFeed(databaseId, containerId, viewsContainerId, auxContainerId, ?logSnapshotConfig) = async {
        let! cosmosClient, contexts = x.Connect("Main", databaseId, containerId, viewsContainerId, auxContainerId, ?logSnapshotConfig = logSnapshotConfig)
        let source, leases = CosmosStoreConnector.getSourceAndLeases cosmosClient databaseId containerId auxContainerId
        return contexts, source, leases }

    /// Indexer Sync mode: When using a ReadOnly connection string, the leases need to be maintained alongside the target
    member x.ConnectWithFeedReadOnly(databaseId, containerId: string, viewsContainerId, auxClient, auxDatabaseId, auxContainerId) = async {
        let! client, contexts = x.Connect("Main", databaseId, containerId, viewsContainerId = viewsContainerId)
        let source = CosmosStoreConnector.getSource client databaseId containerId
        let leases = CosmosStoreConnector.getLeases auxClient auxDatabaseId auxContainerId
        return contexts, source, leases }

    /// Indexer Sync mode: Connects to an External Store that we want to Sync into
    member x.ConnectExternal(role, databaseId, containerId) = async {
        let! client = x.Connect(role, databaseId, [| containerId |])
        return client.CreateContext(role, databaseId, containerId, tipMaxEvents = 128) }

type Factory private () =
    
    static member StartSink(log, stats, maxConcurrentStreams, handle, maxReadAhead) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats)

module OutcomeKind =

    let [<return: Struct>] (|StoreExceptions|_|) (exn: exn) =
        match exn with
        | Equinox.CosmosStore.Exceptions.RateLimited -> Propulsion.Streams.OutcomeKind.RateLimited |> ValueSome
        | Equinox.CosmosStore.Exceptions.RequestTimeout -> Propulsion.Streams.OutcomeKind.Timeout |> ValueSome
        | :? System.Threading.Tasks.TaskCanceledException -> Propulsion.Streams.OutcomeKind.Timeout |> ValueSome
        | _ -> ValueNone

// A typical app will likely have health checks etc, implying the wireup would be via `endpoints.MapMetrics()` and thus not use this ugly code directly
let startMetricsServer port: IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() }
