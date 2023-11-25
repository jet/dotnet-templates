module IndexerTemplate.Indexer.Program

open App
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
        | [<CliPrefix(CliPrefix.None); Last>] Index of ParseResults<CosmosParameters>
        | [<CliPrefix(CliPrefix.None); Last>] Snapshot of ParseResults<CosmosParameters>
        | [<CliPrefix(CliPrefix.None); Last>] Sync of ParseResults<SyncParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 2."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | Index _ ->                "Process indexing into the Views Container for the specified Cosmos feed"
                | Snapshot _ ->             "Process updating of snapshots for all traversed streams in the specified Cosmos feed"
                | Sync _ ->                 "Sync into a specified Store for the specified Cosmos feed"
    and Arguments(c: Args.Configuration, p: ParseResults<Parameters>) =
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 2)
        let maxConcurrentProcessors =       p.GetResult(MaxWriters, 8)
        member val Verbose =                p.Contains Parameters.Verbose
        member val PrometheusPort =         p.TryGetResult PrometheusPort
        member val ProcessorName =          p.GetResult ProcessorName
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member x.Cosmos =                   match x.Action with Action.Index c | Action.Snapshot c -> c | Action.Sync s -> s.Source
        member x.ConnectWithFeed(?lsc) =    match x.Action with
                                            | Action.Index c | Action.Snapshot c -> c.ConnectWithFeed(?lsc = lsc)
                                            | Action.Sync s -> s.ConnectWithFeed()
        member val Action =                 match p.GetSubCommand() with
                                            | Parameters.Index p ->     CosmosArguments(c, p) |> Index
                                            | Parameters.Snapshot p ->  CosmosArguments(c, p) |> Snapshot
                                            | Parameters.Sync p ->      SyncArguments(c, p)   |> Sync
                                            | _ -> p.Raise "Must specify a subcommand"
        member x.ActionLabel =              match x.Action with Action.Index _ -> "Indexing" | Action.Snapshot _ -> "Snapshotting" | Action.Sync _ -> "Exporting"
        member x.IsSnapshotting =           match x.Action with Action.Snapshot _ -> true | _ -> false
        member x.ProcessorParams() =        Log.Information("{action}... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            x.ActionLabel, x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
                                            (x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
    and [<NoEquality; NoComparison>] Action = Index of CosmosArguments | Snapshot of CosmosArguments | Sync of SyncArguments
    and [<NoEquality; NoComparison>] SyncParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Mandatory>] Container of string
        | [<AltCommandLine "-a">]           LeaseContainerId of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        | [<AltCommandLine "-kb">]          MaxKiB of int
        | [<CliPrefix(CliPrefix.None); Last>] Source of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Connection _ ->           "specify a connection string for the destination Cosmos account. Default: Same as Source"
                | Database _ ->             "specify a database name for store. Default: Same as Source"
                | Container _ ->            "specify a container name for store."
                | LeaseContainerId _ ->     "store leases in Sync target DB (default: use `-aux` adjacent to the Source Container). Enables the Source to be read via a ReadOnly connection string."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 0."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
                | MaxKiB _ ->               "specify maximum size in KiB to pass to the Sync stored proc (reduce if Malformed Streams due to 413 RequestTooLarge responses). Default: 128."
                | Source _ ->               "Source store from which events are to be consumed via the feed"
    and SyncArguments(c: Args.Configuration, p: ParseResults<SyncParameters>) =
        let source =                        CosmosArguments(c, p.GetResult Source)
        let discovery =                     p.TryGetResult SyncParameters.Connection
                                            |> Option.map Equinox.CosmosStore.Discovery.ConnectionString
                                            |> Option.defaultWith (fun () -> source.Discovery)
        let timeout =                       p.GetResult(SyncParameters.Timeout, 5) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(SyncParameters.Retries, 1)
        let maxRetryWaitTime =              p.GetResult(SyncParameters.RetriesWaitTime, 5) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime)
        let database =                      p.TryGetResult SyncParameters.Database |> Option.defaultWith (fun () -> source.Database)
        let container =                     p.GetResult SyncParameters.Container
        member val MaxBytes =               p.GetResult(MaxKiB, 128) * 1024
        member val Source =                 source
        member _.ConnectWithFeed() =        match p.TryGetResult LeaseContainerId with
                                            | Some localAuxContainerId -> source.ConnectWithFeedReadOnly(connector.CreateUninitialized(), database, localAuxContainerId)
                                            | None -> source.ConnectWithFeed()
        member _.Connect() = async {        let! context = connector.ConnectExternal("Destination", database, container)
                                            return Equinox.CosmosStore.Core.EventsContext(context, Store.Metrics.log) }
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-v">]           Views of string
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
                | Connection _ ->           $"specify a connection string for a Cosmos account. (optional if environment variable {Args.CONNECTION} specified)"
                | Database _ ->             $"specify a database name for store. (optional if environment variable {Args.DATABASE} specified)"
                | Container _ ->            $"specify a container name for store. (optional if environment variable {Args.CONTAINER} specified)"
                | Views _ ->                $"specify a container name for views. (optional if environment variable {Args.VIEWS} specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | Verbose ->                "request Verbose Logging from ChangeFeedProcessor and Store. Default: off"
                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `<SourceContainer>` + `-aux`."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to request from the feed. Default: unlimited."
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: 1"
    and CosmosArguments(c: Args.Configuration, p: ParseResults<CosmosParameters>) =
        let discovery =                     p.TryGetResult CosmosParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let timeout =                       p.GetResult(Timeout, 5) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.TryGetResult Database  |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   p.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        let viewsContainerId =              p.TryGetResult Views     |> Option.defaultWith (fun () -> c.CosmosViews)

        let leaseContainerId =              p.GetResult(LeaseContainer, containerId + "-aux")
        let fromTail =                      p.Contains FromTail
        let maxItems =                      p.TryGetResult MaxItems
        let lagFrequency =                  p.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member val Verbose =                p.Contains Verbose
        member val MonitoringParams =       fromTail, maxItems, lagFrequency
        member _.Discovery =                discovery
        member _.Database =                 database
        member _.ConnectWithFeed(?lsc) =    connector.ConnectWithFeed(database, containerId, viewsContainerId, leaseContainerId, ?logSnapshotConfig = lsc)
        member _.ConnectWithFeedReadOnly(auxClient, auxDatabase, auxContainerId) =
                                            connector.ConnectWithFeedReadOnly(database, containerId, viewsContainerId, auxClient, auxDatabase, auxContainerId)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Args.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "IndexerTemplate"

let build (args: Args.Arguments) = async {
    let processorName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let! contexts, monitored, leases = args.ConnectWithFeed(args.IsSnapshotting)
    let store = (contexts, Equinox.Cache(AppName, sizeMb = 10)) ||> Store.Cosmos.createConfig
    let parseFeedDoc, sink =
        let mkParseAll () = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereCategory (fun _ -> true)
        let mkSink stats handle = Factory.StartSink(Log.Logger, stats, maxConcurrentStreams, handle, maxReadAhead)
        match args.Action with
        | Args.Action.Index _ ->
            let mkParseCats = Propulsion.CosmosStore.EquinoxSystemTextJsonParser.ofCategories
            let stats = Indexer.Stats(Log.Logger, args.StatsInterval, args.StateInterval, args.Cosmos.Verbose)
            let handle = Indexer.Factory.createHandler store
            mkParseCats Indexer.sourceCategories, mkSink stats handle
        | Args.Action.Snapshot _ ->
            let stats = Snapshotter.Stats(Log.Logger, args.StatsInterval, args.StateInterval, args.Cosmos.Verbose)
            let handle = Snapshotter.Factory.createHandler store
            mkParseAll (), mkSink stats handle
        | Args.Action.Sync a ->
            mkParseAll (),
            let eventsContext = a.Connect() |> Async.RunSynchronously
            let stats = Propulsion.CosmosStore.CosmosStoreSinkStats(Log.Logger, args.StatsInterval, args.StateInterval)
            Propulsion.CosmosStore.CosmosStoreSink.Start(
                Log.Logger, maxReadAhead, eventsContext, maxConcurrentStreams, stats,
                purgeInterval = TimeSpan.FromHours 1, maxBytes = a.MaxBytes)
    let source =
        let startFromTail, maxItems, lagFrequency = args.Cosmos.MonitoringParams
        Propulsion.CosmosStore.CosmosStoreSource(Log.Logger, args.StatsInterval, monitored, leases, processorName, parseFeedDoc, sink,
                                                 startFromTail = startFromTail, ?maxItems = maxItems,  lagEstimationInterval = lagFrequency).Start()
    return sink, source }

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let run args = async {
    let! sink, source = build args
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
            with e when not (e :? Args.MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn $"%s{msg}"; 1
        | :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"Exception %s{e.Message}"; 1
