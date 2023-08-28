module IndexerTemplate.Indexer.Program

open IndexerTemplate
open Serilog
open System

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 2."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | Cosmos _ ->               "specify CosmosDB input parameters"
    and Arguments(c: App.Configuration, p: ParseResults<Parameters>) =
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 2)
        let maxConcurrentProcessors =       p.GetResult(MaxWriters, 8)
        member val Verbose =                p.Contains Parameters.Verbose
        member val PrometheusPort =         p.TryGetResult PrometheusPort
        member val ProcessorName =          p.GetResult ProcessorName
        member x.ProcessorParams() =        Log.Information("Reacting... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
                                            (x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val Cosmos =                 CosmosArguments(c, p.GetResult Cosmos)
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float

        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-b"; Unique>]   MaxItems of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | Verbose ->                "request Verbose Logging from ChangeFeedProcessor and Store. Default: off"
                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to request from the feed. Default: unlimited."
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: 1"
    and CosmosArguments(c: App.Configuration, p: ParseResults<CosmosParameters>) =
        let discovery =                     p.TryGetResult CosmosParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let timeout =                       p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.TryGetResult Database  |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   p.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)

        let leaseContainerId =              p.GetResult(LeaseContainer, containerId + "-aux")
        let fromTail =                      p.Contains FromTail
        let maxItems =                      p.TryGetResult MaxItems
        let lagFrequency =                  p.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member val Verbose =                p.Contains Verbose
        member val MonitoringParams =       fromTail, maxItems, lagFrequency
        member _.ConnectWithFeed() =        connector.ConnectWithFeed(database, containerId, leaseContainerId)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(App.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "IndexerTemplate"

let build (args: Args.Arguments) =
    let processorName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let context, monitored, leases = args.Cosmos.ConnectWithFeed() |> Async.RunSynchronously
    let sink =
        let store =
            let cache = Equinox.Cache(AppName, sizeMb = 10)
            Store.Config.Cosmos (context, cache)
        let stats = Indexer.Stats(Log.Logger, args.StatsInterval, args.StateInterval, args.Cosmos.Verbose)
        let handle = Indexer.Factory.createHandler store
        Factory.StartSink(Log.Logger, stats, maxConcurrentStreams, handle, maxReadAhead)
    let source =
        let parseFeedDoc = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.enumCategoryEvents Indexer.reactionCategories
        let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Seq.collect parseFeedDoc)
        let startFromTail, maxItems, lagFrequency = args.Cosmos.MonitoringParams
        Propulsion.CosmosStore.CosmosStoreSource.Start(Log.Logger, monitored, leases, processorName, observer,
                                                       startFromTail = startFromTail, ?maxItems = maxItems, lagReportFreq = lagFrequency)
    sink, source

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let run args = async {
    let sink, source = build args
    use _metricsServer: IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    return! [|   Async.AwaitKeyboardInterruptAsTaskCanceledException()
                 source.AwaitWithStopOnCancellation()
                 sink.AwaitWithStopOnCancellation()
            |] |> Async.Parallel |> Async.Ignore<unit[]> }

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try let metrics = Sinks.equinoxAndPropulsionCosmosConsumerMetrics (Sinks.tags AppName) args.ProcessorName
            Log.Logger <- LoggerConfiguration().Configure(args.Verbose).Sinks(metrics, args.Cosmos.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? App.MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with App.MissingArg msg -> eprintfn $"%s{msg}"; 1
        | :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"Exception %s{e.Message}"; 1
