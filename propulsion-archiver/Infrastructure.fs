[<AutoOpen>]
module ArchiverTemplate.Infrastructure

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
        a.Logger(fun l ->
            l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
              |> fun l -> if not metrics then l else
                            l.WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(appName))
                             .WriteTo.Sink(Propulsion.Prometheus.LogSink(["app", appName], group))
                             .WriteTo.Sink(Propulsion.CosmosStore.Prometheus.LogSink(["app",appName]))
              |> ignore) |> ignore

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, appName, group, verbose, (logSyncToConsole, minRu), cfpVerbose, metrics) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose then c.MinimumLevel.Debug() else c
        |> fun c -> c.ConfigureChangeFeedProcessorLogging(cfpVerbose)
        |> fun c -> let ingesterLevel = if logSyncToConsole then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamSchedulingEngine>.FullName, ingesterLevel)
        |> fun c -> let generalLevel = if verbose then Events.LogEventLevel.Information else Events.LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.CosmosStore.Internal.Writer.Result>.FullName, generalLevel)
        |> fun c ->
            let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {Properties}{NewLine}{Exception}"
            let t = if verbose then t else t.Replace("{Properties}", "")
            let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                a.ForwardMetricsFromEquinoxAndPropulsionToPrometheus(appName, group, metrics)
                a.Logger(fun l ->
                    let isEqx = Filters.Matching.FromSource<Equinox.CosmosStore.CosmosStoreContext>().Invoke
                    let isWriterB = Filters.Matching.FromSource<Propulsion.CosmosStore.Internal.Writer.Result>().Invoke
                    let l = if logSyncToConsole then l else l.Filter.ByExcluding(fun x -> isEqx x || isWriterB x)
                    let isCheaperThan minRu = function
                        | Equinox.CosmosStore.Core.Log.MetricEvent
                            (Equinox.CosmosStore.Core.Log.Metric.SyncSuccess m
                                | Equinox.CosmosStore.Core.Log.Metric.SyncConflict m
                                | Equinox.CosmosStore.Core.Log.Metric.SyncResync m) ->
                            m.ru < minRu
                        | _ -> false
                    let l = match minRu with Some mru -> l.Filter.ByExcluding(fun x -> isCheaperThan mru x) | None -> l
                    l.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t) |> ignore)
                |> ignore
            c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)
