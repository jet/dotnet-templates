module Reactor.Program

open Infrastructure
open Serilog
open System

module Store = Domain.Store

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-t"; Unique>]   TimeoutS of float
        
        | [<AltCommandLine "-i"; Unique>]   IdleDelayMs of int
        | [<AltCommandLine "-W"; Unique>]   WakeForResults

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Mdb of ParseResults<SourceArgs.Mdb.Parameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."

                | IdleDelayMs _ ->          "Idle delay for scheduler. Default 1000ms"
                | WakeForResults _ ->       "Wake for all results to provide optimal throughput"

                | Dynamo _ ->               "specify DynamoDB input parameters"
                | Mdb _ ->                  "specify MessageDb input parameters"
    and Arguments(c : SourceArgs.Configuration, p : ParseResults<Parameters>) =
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 16)
        let maxConcurrentProcessors =       p.GetResult(MaxWriters, 8)
        member val ProcessorName =          p.GetResult ProcessorName
        member val PrometheusPort =         p.TryGetResult PrometheusPort
        member val Verbose =                p.Contains Parameters.Verbose
        member val CacheSizeMb =            10
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 10.
        member val PurgeInterval =          TimeSpan.FromHours 1.
        member val IdleDelay =              p.GetResult(IdleDelayMs, 1000) |> TimeSpan.FromMilliseconds
        member val WakeForResults =         p.Contains WakeForResults
        member x.ProcessorParams() =        Log.Information("Reacting... {processorName}, reading {maxReadAhead} ahead, {dop} streams",
                                                            x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
                                            (x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
        member val Store : Choice<SourceArgs.Dynamo.Arguments, SourceArgs.Mdb.Arguments> =
                                            match p.GetSubCommand() with
                                            | Dynamo a -> Choice1Of2 <| SourceArgs.Dynamo.Arguments(c, a)
                                            | Mdb a ->    Choice2Of2 <| SourceArgs.Mdb.Arguments(c, a)
                                            | a ->        Args.missingArg $"Unexpected Store subcommand %A{a}"
        member x.ConnectStoreAndSource(appName) : Store.Context * (ILogger -> string -> SourceConfig) * (ILogger -> unit) =
            let cache = Equinox.Cache (appName, sizeMb = x.CacheSizeMb)
            match x.Store with
            | Choice1Of2 a ->
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let indexStore, startFromTail, batchSizeCutoff, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, cache)
                    let load = Propulsion.DynamoStore.EventLoadMode.IndexOnly
                    SourceConfig.Dynamo (indexStore, checkpoints, load, startFromTail, batchSizeCutoff, tailSleepInterval, x.StatsInterval)
                let store = Store.Context.Dynamo (context, cache)
                store, buildSourceConfig, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
            | Choice2Of2 a ->
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let connectionString, startFromTail, batchSize, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName)
                    SourceConfig.Mdb (connectionString, checkpoints, startFromTail, batchSize, tailSleepInterval, x.StatsInterval)
                let store = Store.Context.Mdb (context, cache)
                store, buildSourceConfig, Equinox.MessageDb.Log.InternalMetrics.dump

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(SourceArgs.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "Reactor"

let build (args : Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let store, buildSourceConfig, dumpMetrics = args.ConnectStoreAndSource(AppName)
    let log = Log.Logger
    let sink =
        let stats = Handler.Stats(log, args.StatsInterval, args.StateInterval, dumpMetrics)
        let handle = Handler.create store
        Handler.Factory.StartSink(log, stats, maxConcurrentStreams, handle, maxReadAhead, 
                                 wakeForResults = args.WakeForResults, idleDelay = args.IdleDelay, purgeInterval = args.PurgeInterval)
    let source, _awaitReactions =
        let sourceConfig = buildSourceConfig log consumerGroupName
        Handler.Factory.StartSource(log, sink, sourceConfig)
    sink, source

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

// A typical app will likely have health checks etc, implying the wireup would be via `endpoints.MapMetrics()` and thus not use this ugly code directly
let startMetricsServer port : IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() }

let run args = async {
    let sink, source = build args
    use _ = source
    use _ = sink
    use _metricsServer : IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    return! Async.Parallel [ Async.AwaitKeyboardInterruptAsTaskCanceledException()
                             source.AwaitWithStopOnCancellation()
                             sink.AwaitWithStopOnCancellation() ] |> Async.Ignore<unit[]> }

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try let metrics = Sinks.equinoxAndPropulsionFeedMetrics (Sinks.tags AppName) args.ProcessorName
            Log.Logger <- LoggerConfiguration().Configure(args.Verbose).Sinks(metrics, args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? Args.MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
