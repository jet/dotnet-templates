[<AutoOpen>]
module ArchiverTemplate.Infrastructure

open Serilog
open System.Runtime.CompilerServices

[<Extension>]
type LoggerConfigurationExtensions() =

    [<Extension>]
    static member inline ExcludeChangeFeedProcessorV2InternalDiagnostics(c : LoggerConfiguration) =
        let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
        c.Filter.ByExcluding(fun x -> isCfp x)

    [<Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        // LibLog writes to the global logger, so we need to control the emission
        let cfpl = if verbose then Events.LogEventLevel.Debug else Events.LogEventLevel.Warning
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
        |> fun c -> if verbose then c else c.ExcludeChangeFeedProcessorV2InternalDiagnostics()

module EquinoxCosmosStorePrometheusMetrics =

    module Histograms =

        let private mkHistogram (cfg : Prometheus.HistogramConfiguration) name desc =
            let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
            fun (facet : string, op : string) (app : string, cat : string) s -> h.WithLabels(facet, op, app, cat).Observe(s)
        let labelNames = [| "facet"; "op"; "app"; "cat" |]
        let private sHistogram =
            let sBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8. |]
            let sCfg = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = labelNames)
            mkHistogram sCfg
        let private ruHistogram =
            let ruBuckets = Prometheus.Histogram.ExponentialBuckets(1., 2., 11) // 1 .. 1024
            let ruCfg = Prometheus.HistogramConfiguration(Buckets = ruBuckets, LabelNames = labelNames)
            mkHistogram ruCfg
        let private sAndRuPair stat desc =
            let baseName = "equinox_" + stat
            let baseDesc = "Equinox CosmosDB " + desc
            let observeS = sHistogram (baseName + "_seconds") (baseDesc + " latency")
            let observeRu = ruHistogram (baseName + "_ru") (baseDesc + " charge")
            fun (facet, op) app (cat, s, ru) ->
                observeS (facet, op) (app, cat) s
                observeRu (facet, op) (app, cat) ru
        let op = sAndRuPair "op" "Operation"
        let res = sAndRuPair "roundtrip" "Fragment"

    module Counters =

        let private mkCounter (cfg : Prometheus.CounterConfiguration) name desc =
            let h = Prometheus.Metrics.CreateCounter(name, desc, cfg)
            fun (facet : string, op : string, outcome : string) (app : string, cat : string) c -> h.WithLabels(facet, op, outcome, app, cat).Inc(c)
        let labelNames = [| "facet"; "op"; "outcome"; "app"; "cat" |]
        let private counter =
            let cCfg = Prometheus.CounterConfiguration(LabelNames = labelNames)
            mkCounter cCfg
        let private eventsAndBytesPair stat desc =
            let baseName = "equinox_" + stat
            let baseDesc = "Equinox CosmosDB " + desc
            let observeE = counter (baseName + "_events") (baseDesc + "Events")
            let observeB = counter (baseName + "_bytes") (baseDesc + "Bytes")
            fun ctx app (cat, e, b) ->
                observeE ctx (app, cat) e
                match b with None -> () | Some b -> observeB ctx (app, cat) b
        let size = eventsAndBytesPair "payload" "Payload, "

    module Summaries =

        let labelNames = [| "facet"; "app" |]

        let mkSummary (cfg : Prometheus.SummaryConfiguration) name desc  =
            let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
            fun (facet : string) (app : string) o -> s.WithLabels(facet, app).Observe(o)
        let cfg =
            let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
            let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
            Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = labelNames, MaxAge = System.TimeSpan.FromMinutes 1.)
        let sAndRuPair stat desc =
            let baseName = "equinox_" + stat
            let baseDesc = "Equinox CosmosDB " + desc
            let observeS = mkSummary cfg (baseName + "_seconds") (baseDesc + " latency")
            let observeRu = mkSummary cfg (baseName + "_ru") (baseDesc + " charge")
            fun facet app (s, ru) ->
                observeS facet app s
                observeRu facet app ru
        let op = sAndRuPair "op_summary" "Operation Summary"
        let res = sAndRuPair "roundtrip_summary" "Fragment Summary"

    open Equinox.CosmosStore.Core.Log

    let inline (|CatSRu|) ({ interval = i; ru = ru; stream = s }: Measurement) =
        let cat, _id = FsCodec.StreamName.splitCategoryAndId (FSharp.UMX.UMX.tag s)
        let s = let e = i.Elapsed in e.TotalSeconds
        cat, s, ru
    let observeSRu (facet, _op as stat) app (_cat, s, ru as catSRu) =
        Histograms.op stat app catSRu
        Summaries.op facet app (s, ru)
    let observeOp stat app (CatSRu catSRu) =
        observeSRu stat app catSRu
    let observeOpp (facet, op, outcome) app (CatSRu ((cat, _s, _ru) as catSRu) as m) =
        observeSRu (facet, op) app catSRu
        Counters.size (facet, op, outcome) app (cat, float m.count, if m.bytes = -1 then None else Some (float m.bytes))
    let observeRes (facet, _op as stat) app (CatSRu ((_cat, s, ru) as catSRu)) =
        Histograms.res stat app catSRu
        Summaries.res facet app (s, ru)

    type LogSink(app) =
        interface Serilog.Core.ILogEventSink with
            member __.Emit logEvent =
                match logEvent with
                | MetricEvent cm ->
                    match cm with
                    | Op       (Operation.Tip,      m) -> observeOpp  ("query",    "tip",           "ok")       app m
                    | Op       (Operation.Tip404,   m) -> observeOpp  ("query",    "tip",           "ok")       app m
                    | Op       (Operation.Tip302,   m) -> observeOpp  ("query",    "tip",           "cached")   app m
                    | Op       (Operation.Query,    m) -> observeOpp  ("query",    "query",         "ok")       app m
                    | QueryRes (_direction,         m) -> observeRes  ("query",    "queryPage")                 app m
                    | Op       (Operation.Write,    m) -> observeOpp  ("transact", "sync",          "ok")       app m
                    | Op       (Operation.Conflict, m) -> observeOpp  ("transact", "conflict",      "conflict") app m
                    | Op       (Operation.Resync,   m) -> observeOpp  ("transact", "resync",        "conflict") app m
                    | Op       (Operation.Prune,    m) -> observeOp   ("prune",    "pruneQuery")                app m
                    | PruneRes (                    m) -> observeRes  ("prune",    "pruneQueryPage")            app m
                    | Op       (Operation.Delete,   m) -> observeOpp  ("prune",    "delete",        "ok")       app m
                    | Op       (Operation.Trim,     m) -> observeOpp  ("prune",    "trim",          "ok")       app m
                | _ -> ()

[<Extension>]
type Logging() =

    [<Extension>]
    static member Configure(configuration : LoggerConfiguration, appName, verbose, (logSyncToConsole, minRu), cfpVerbose) =
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
                a.Logger(fun l ->
                    l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.LogSink())
                     .WriteTo.Sink(EquinoxCosmosStorePrometheusMetrics.LogSink(appName))
                     |> ignore) |> ignore
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
