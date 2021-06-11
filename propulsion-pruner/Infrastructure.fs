[<AutoOpen>]
module PrunerTemplate.Infrastructure

open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

[<System.Runtime.CompilerServices.Extension>]
type LoggerConfigurationExtensions() =

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ExcludeChangeFeedProcessorV2InternalDiagnostics(c : LoggerConfiguration) =
        let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
        c.Filter.ByExcluding(fun x -> isCfp x)

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        // LibLog writes to the global logger, so we need to control the emission
        let cfpl = if verbose then Events.LogEventLevel.Debug else Events.LogEventLevel.Warning
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
        |> fun c -> if verbose then c else c.ExcludeChangeFeedProcessorV2InternalDiagnostics()

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ForwardMetricsFromEquinoxAndPropulsionToPrometheus(a : Configuration.LoggerSinkConfiguration, appName, group, metrics) =
        let customTags = ["app", appName]
        a.Logger(fun l ->
            l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
              |> fun l -> if not metrics then l else
                            l.WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(customTags))
                             .WriteTo.Sink(Propulsion.Prometheus.LogSink(customTags, group))
                             .WriteTo.Sink(Propulsion.CosmosStore.Prometheus.LogSink(customTags))
              |> ignore) |> ignore

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, appName, group, ?verbose, ?changeFeedProcessorVerbose, ?metrics) =
        let verbose, cfpVerbose = defaultArg verbose false, defaultArg changeFeedProcessorVerbose false
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose then c.MinimumLevel.Debug() else c
        |> fun c -> c.ConfigureChangeFeedProcessorLogging(cfpVerbose)
        |> fun c -> let ingesterLevel = if cfpVerbose then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamSchedulingEngine>.FullName, ingesterLevel)
        |> fun c -> let generalLevel = if verbose then Events.LogEventLevel.Information else Events.LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.CosmosStore.Internal.Writer.Result>.FullName, generalLevel)
        |> fun c ->
            let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {Properties}{NewLine}{Exception}"
            let t = if verbose then t else t.Replace("{Properties}", "")
            let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                a.ForwardMetricsFromEquinoxAndPropulsionToPrometheus(appName, group, (metrics = Some true))
                a.Logger(fun l ->
                    let isEqx = Filters.Matching.FromSource<Equinox.CosmosStore.Core.EventsContext>().Invoke
                    let isWriterB = Filters.Matching.FromSource<Propulsion.CosmosStore.Internal.Writer.Result>().Invoke
                    let l = if cfpVerbose then l else l.Filter.ByExcluding(fun x -> isEqx x || isWriterB x)
                    l.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t) |> ignore)
                |> ignore
            c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)

open Equinox.CosmosStore
open Microsoft.Azure.Cosmos

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

/// Manages establishing a CosmosClient, which is used by CosmosStoreClient to read from the underlying Cosmos DB Container.
type CosmosStoreConnector
    (   /// CosmosDB endpoint/credentials specification.
        discovery : Discovery,
        /// Timeout to apply to individual reads/write round-trips going to CosmosDB. CosmosDB Default: 1m.
        requestTimeout: TimeSpan,
        /// Maximum number of times to attempt when failure reason is a 429 from CosmosDB, signifying RU limits have been breached. CosmosDB default: 9
        maxRetryAttemptsOnRateLimitedRequests: int,
        /// Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDB in the 429 response). CosmosDB default: 30s
        maxRetryWaitTimeOnRateLimitedRequests: TimeSpan,
        /// Connection mode (default: ConnectionMode.Direct (best performance, same as Microsoft.Azure.Cosmos SDK default)
        /// NOTE: default for Equinox.Cosmos.Connector (i.e. V2) was Gateway (worst performance, least trouble, Microsoft.Azure.DocumentDb SDK default)
        [<O; D(null)>]?mode : ConnectionMode,
        /// Connection limit for Gateway Mode. CosmosDB default: 50
        [<O; D(null)>]?gatewayModeMaxConnectionLimit,
        /// consistency mode (default: ConsistencyLevel.Session)
        [<O; D(null)>]?defaultConsistencyLevel : ConsistencyLevel,
        /// Inhibits certificate verification when set to <c>true</c>, i.e. for working with the CosmosDB Emulator (default <c>false</c>)
        [<O; D(null)>]?bypassCertificateValidation : bool) =

    let factory =
        CosmosClientFactory
          ( requestTimeout, maxRetryAttemptsOnRateLimitedRequests, maxRetryWaitTimeOnRateLimitedRequests,
            ?gatewayModeMaxConnectionLimit = gatewayModeMaxConnectionLimit, ?mode = mode, ?defaultConsistencyLevel = defaultConsistencyLevel,
            ?bypassCertificateValidation = bypassCertificateValidation)

    /// The <c>CosmosClientOptions</c> used when connecting to CosmosDB
    member _.Options = factory.Options

    /// The Endpoint Uri for the target CosmosDB
    member _.Endpoint = discovery.Endpoint

    /// Creates an instance of CosmosClient without actually validating or establishing the connection
    /// It's recommended to use <c>Connect()</c> and/or <c>CosmosStoreClient.Connect()</c> in preference to this API
    ///   in order to avoid latency spikes, and/or deferring discovery of connectivity or permission issues.
    member _.CreateUninitialized() = factory.CreateUninitialized(discovery)

    /// Creates and validates a Client [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member _.Connect(containers) = factory.CreateAndInitialize(discovery, containers)

//type Equinox.CosmosStore.CosmosStoreConnector with

    member x.LogConfiguration(log : ILogger, connectionName, database, container) =
        let o = x.Options
        let timeout, timeout429, retries429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        log.Information("CosmosDb {name} {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        connectionName, o.ConnectionMode, x.Endpoint, timeout.TotalSeconds, retries429, timeout429)
        log.Information("CosmosDb {name} Database {database} Container {container}",
                        connectionName, database, container)

    /// Prefer <c>Connect</c> in order to optimize first request latency
    member x.CreateUninitialized(databaseId, containerId) =
        x.CreateUninitialized().GetDatabase(databaseId).GetContainer(containerId)
    
    member x.Connect(connectionName, databaseId, containerId) =
        x.LogConfiguration(Log.Logger, connectionName, databaseId, containerId)
        Equinox.CosmosStore.CosmosStoreClient.Connect(x.Connect, databaseId, containerId)
        
    member x.ConnectMonitor(databaseId, containerId) =
        x.LogConfiguration(Log.Logger, "Source", databaseId, containerId)
        x.CreateUninitialized(databaseId, containerId)

