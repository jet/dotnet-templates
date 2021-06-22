[<AutoOpen>]
module PrunerTemplate.Infrastructure

open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

[<System.Runtime.CompilerServices.Extension>]
type LoggerConfigurationExtensions() =

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        let cfpl = if verbose then Events.LogEventLevel.Debug else Events.LogEventLevel.Warning
        // TODO figure out what CFP v3 requires
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)

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
            let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {Properties}{NewLine}{Exception}"
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
    member x.ConnectMonitored(databaseId, containerId) =
        x.LogConfiguration("Source", databaseId, containerId)
        x.CreateUninitialized(databaseId, containerId)

