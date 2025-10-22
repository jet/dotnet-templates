module IndexerTemplate.Indexer.Program

open App
open Serilog
open System

module Args =

    open Argu
    [<NoEquality; NoComparison; RequireSubcommand>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-a"; Unique>]   AbendTimeoutM of float

        | [<Unique>] DryRun
        | [<AltCommandLine "-F";    Unique>] Follow
        | [<AltCommandLine "-I";    AltCommandLine "--include-indexes"; Unique>] IncIdx
        | [<AltCommandLine "-cat";  AltCommandLine "--include-category">]   IncCat of    regex: string
        | [<AltCommandLine "-ncat"; AltCommandLine "--exclude-category">]   ExcCat of    regex: string
        | [<AltCommandLine "-sn";   AltCommandLine "--include-streamname">] IncStream of regex: string
        | [<AltCommandLine "-nsn";  AltCommandLine "--exclude-streamname">] ExcStream of regex: string
        | [<AltCommandLine "-et";   AltCommandLine "--include-eventtype">]  IncEvent of  regex: string
        | [<AltCommandLine "-net";  AltCommandLine "--exclude-eventtype">]  ExcEvent of  regex: string

        | [<CliPrefix(CliPrefix.None); Last>] Stats of ParseResults<Args.CosmosSourceParameters>
        | [<CliPrefix(CliPrefix.None); Last>] Index of ParseResults<IndexParameters>
        | [<CliPrefix(CliPrefix.None); Last>] Snapshot of ParseResults<Args.CosmosSourceParameters>
        | [<CliPrefix(CliPrefix.None); Last>] Sync of ParseResults<SyncParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: File: 32768 Cosmos: 2."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8 (Sync, Index: 16)."
                | AbendTimeoutM _ ->        "maximum number of minutes to wait before existing where processing enters a (non-transient) perpetual exception state. Default: 2"

                | DryRun ->                 "For Snapshot subcommand, skip actually updating"
                | Follow ->                 "Continue waiting for more input when complete (like unix `tail -f`). Default: the Snapshot and Stats operations exit when the Tail of the feed has been reached"

                | IncIdx ->                 "Include Index streams. Default: Exclude Index Streams, identified by a $ prefix."
                | IncCat _ ->               "Allow Stream Category. Multiple values are combined with OR. Default: include all, subject to Category Deny and Stream Deny rules."
                | ExcCat _ ->               "Deny  Stream Category. Specified values/regexes are applied after the Category Allow rule(s)."
                | IncStream _ ->            "Allow Stream Name. Multiple values are combined with OR. Default: Allow all streams that pass the category Allow test, Fail the Category and Stream deny tests."
                | ExcStream _ ->            "Deny  Stream Name. Specified values/regexes are applied after the IncCat, ExcCat and IncStream filters."

                | IncEvent _ ->             "Allow Event Type Name. Multiple values are combined with OR. Applied only after Category and Stream filters. Default: include all."
                | ExcEvent _ ->             "Deny  Event Type Name. Specified values/regexes are applied after the Event Type Name Allow rule(s)."

                | Stats _ ->                "Gather stats from the input data only; No indexing or writes performed."
                | Index _ ->                "Process indexing into the Views Container for the specified feed"
                | Snapshot _ ->             "Process updating of snapshots for all traversed streams in the specified Cosmos feed"
                | Sync _ ->                 "Sync into a specified Store from the specified Cosmos feed"
    and StreamFilterArguments(p: ParseResults<Parameters>) =
        let allowCats, denyCats = p.GetResults IncCat, p.GetResults ExcCat
        let allowSns, denySns = p.GetResults IncStream, p.GetResults ExcStream
        let incIndexes = p.Contains IncIdx
        let allowEts, denyEts = p.GetResults IncEvent, p.GetResults ExcEvent
        let isPlain = Seq.forall (fun x -> Char.IsLetterOrDigit x || x = '_')
        let asRe = Seq.map (fun x -> if isPlain x then $"^%s{x}$" else x)
        let (|Filter|) exprs =
            let values, pats = List.partition isPlain exprs
            let valuesContains = let set = System.Collections.Generic.HashSet(values) in set.Contains
            let aPatternMatches (x: string) = pats |> List.exists (fun p -> System.Text.RegularExpressions.Regex.IsMatch(x, p))
            fun cat -> valuesContains cat || aPatternMatches cat
        let filter map (allow, deny) =
            match allow, deny with
            | [], [] -> fun _ -> true
            | Filter includes, Filter excludes -> fun x -> let x = map x in (List.isEmpty allow || includes x) && not (excludes x)
        let validStream = filter FsCodec.StreamName.toString (allowSns, denySns)
        let isTransactionalStream (sn: FsCodec.StreamName) = let sn = FsCodec.StreamName.toString sn in not (sn.StartsWith('$'))
        member _.CreateStreamFilter(maybeCategories) =
            let handlerCats = match maybeCategories with Some xs -> List.ofArray xs | None -> List.empty
            let allowCats = handlerCats @ allowCats
            let validCat = filter FsCodec.StreamName.Category.ofStreamName (allowCats, denyCats)
            let allowCats = match allowCats with [] -> [ ".*" ] | xs -> xs
            let denyCats = denyCats @ [ if not incIndexes then "^\$" ]
            let allowSns, denySns = match allowSns, denySns with [], [] -> [".*"], [] | x -> x
            let allowEts, denyEts = match allowEts, denyEts with [], [] -> [".*"], [] | x -> x
            Log.Information("Categories ☑️ {@allowCats} 🚫{@denyCats} Streams ☑️ {@allowStreams} 🚫{denyStreams} Events ☑️ {allowEts} 🚫{@denyEts}",
                            asRe allowCats, asRe denyCats, asRe allowSns, asRe denySns, asRe allowEts, asRe denyEts)
            fun sn ->
                validCat sn
                && validStream sn
                && (incIndexes || isTransactionalStream sn)
        member val EventFilter = filter (fun (x: Propulsion.Sinks.Event) -> x.EventType) (allowEts, denyEts)
    and [<NoComparison; NoEquality>] Action =
        | Index of IndexArguments
        | Snapshot of Args.CosmosSourceArguments
        | Sync of SyncArguments
    and Arguments(c: Args.Configuration, p: ParseResults<Parameters>) =
        let action =                        match p.GetSubCommand() with
                                            | Parameters.Index p ->     Index         <| IndexArguments(c, p)
                                            | Parameters.Snapshot p ->  Snapshot      <| Args.CosmosSourceArguments(c, p)
                                            | Parameters.Sync p ->      Sync          <| SyncArguments(c, p)
                                            | _ -> p.Raise "Must specify a subcommand"
        let source =                        match action with
                                            | Snapshot c ->                             c
                                            | Index a ->                                a.Source
                                            | Sync s ->                                 s.Source
        let dryRun =                        match action, p.Contains DryRun with
                                            | Snapshot _, value -> value
                                            | _, true -> p.Raise "dryRun is not applicable to any subcommand other than Snapshot"
                                            | _, false -> false
        let actionLabel =                   match action with
                                            | Snapshot _ when dryRun -> "DryRun Snapshot inspect"
                                            | Snapshot _ -> "Snapshot updat"
                                            | Index _ -> "Index"
                                            | Sync _ -> "Synchroniz"
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 2)
        member val Action =                 action
        member val DryRun =                 dryRun
        member val Source =                 source
        member val Verbose =                p.Contains Verbose
        member val PrometheusPort =         p.TryGetResult PrometheusPort
        member val ProcessorName =          p.GetResult ProcessorName
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val AbendTimeout =           p.GetResult(AbendTimeoutM, 2.) |> TimeSpan.FromMinutes
        member val Filters =                StreamFilterArguments(p)
        member val MaxConcurrentProcessors =p.GetResult(MaxWriters, match action with Sync _ | Index _ -> 16 | _ -> 8)
        member val CosmosVerbose =          source.Verbose
        member x.WaitForTail =              if p.Contains Follow then None
                                            else Some (x.StatsInterval * 2.)
        member x.LagEstimationInterval =    x.WaitForTail |> Option.map (fun _ -> TimeSpan.s 5)
        member x.ProcessorParams() =        Log.Information("{action}ing... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            actionLabel, x.ProcessorName, maxReadAhead, x.MaxConcurrentProcessors)
                                            (x.ProcessorName, maxReadAhead, x.MaxConcurrentProcessors)
        member _.Connect appName = async {  let store contexts = (contexts, Equinox.Cache(appName, sizeMb = 10)) ||> Store.Cosmos.createConfig
                                            match source, action with
                                            | c, action ->
                                                let lsc = match action with Snapshot _ -> true | _ -> false
                                                let! contexts, monitored, leases = c.ConnectWithFeed(lsc = lsc)
                                                return (monitored, leases, c.MonitoringParams, store contexts) }
    and [<NoEquality; NoComparison>] SyncParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Mandatory>] Container of string
        | [<AltCommandLine "-a">]           LeaseContainerId of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        | [<AltCommandLine "-kb">]          MaxKiB of int
        | [<CliPrefix(CliPrefix.None); Last>] Source of ParseResults<Args.CosmosSourceParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Connection _ ->           "specify a connection string for the destination Cosmos account. Default (if Cosmos): Same as Source"
                | Database _ ->             "specify a database name for store. Default (if Cosmos): Same as Source"
                | Container _ ->            "specify a container name for store."
                | LeaseContainerId _ ->     "store leases in Sync target DB (default: use `-aux` adjacent to the Source Container). Enables the Source to be read via a ReadOnly connection string."
                | Retries _ ->              "specify operation retries. Default: 0."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
                | MaxKiB _ ->               "specify maximum size in KiB to pass to the Sync stored proc (reduce if Malformed Streams due to 413 RequestTooLarge responses). Default: 128."
                | Source _ ->               "Source store from which events are to be consumed via the feed"
    and SyncArguments(c: Args.Configuration, p: ParseResults<SyncParameters>) =
        let source =                        match p.GetSubCommand() with
                                            | Source p -> Args.CosmosSourceArguments(c, p)
                                            | x -> p.Raise $"Unexpected Subcommand %A{x}"
        let connection =                    p.GetResult(Connection, fun () -> c.CosmosConnection)
        let connector =
            let retries =                   p.GetResult(Retries, 1)
            let maxRetryWaitTime =          p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
            Equinox.CosmosStore.CosmosStoreConnector(Equinox.CosmosStore.Discovery.ConnectionString connection, retries, maxRetryWaitTime)
        let database =                      p.GetResult(Database, fun () -> c.CosmosDatabase)
        let container =                     p.GetResult Container
        member val Source =                 source
        member val MaxBytes =               p.GetResult(MaxKiB, 128) * 1024
        member x.Connect() =                connector.ConnectExternal("Destination", database, container)
        member x.ConnectEvents() = async {  let! context = x.Connect()
                                            return Equinox.CosmosStore.Core.EventsContext(context, Store.Metrics.log) }
        member x.ConnectWithFeed() =        match p.TryGetResult LeaseContainerId with
                                            | Some localAuxContainerId -> source.ConnectWithFeedReadOnly(connector.CreateUninitialized(), database, localAuxContainerId)
                                            | None -> source.ConnectWithFeed()
    and [<NoEquality; NoComparison>] IndexParameters =
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Args.CosmosSourceParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Cosmos _ ->               "CosmosDb source parameters"
    and IndexArguments(c: Args.Configuration, p: ParseResults<IndexParameters>) =
        member val Source =                 match p.GetSubCommand() with
                                            | IndexParameters.Cosmos p -> Args.CosmosSourceArguments(c, p)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(Args.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "IndexerTemplate"

let build (args: Args.Arguments) = async {
    let processorName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let parse = args.Filters.CreateStreamFilter >> Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereStream
    let configureWithStreamsSink_ stats cats handle =
        cats |> parse, Factory.StartStreamsSink(Log.Logger, stats, maxConcurrentStreams, handle, maxReadAhead)
    let configureWithStreamsSink stats handle = configureWithStreamsSink_ stats None handle
    let index store =
        let stats = Indexer.Stats(Log.Logger, args.StatsInterval, args.StateInterval, args.CosmosVerbose, args.AbendTimeout)
        let cats, handle = Indexer.Factory.create store // args.Filters.EventFilter
        configureWithStreamsSink_ stats cats handle
    let snapshot store =
        let stats = Snapshotter.Stats(Log.Logger, args.StatsInterval, args.StateInterval, args.CosmosVerbose)
        let handle = Snapshotter.Factory.createHandler args.DryRun store
        configureWithStreamsSink stats handle
    let sync (a: Args.SyncArguments) =
        let eventsContext = a.ConnectEvents() |> Async.RunSynchronously
        parse None,
        let stats = Propulsion.CosmosStore.CosmosStoreSinkStats(Log.Logger, args.StatsInterval, args.StateInterval)
        Propulsion.CosmosStore.CosmosStoreSink.Start(Log.Logger, maxReadAhead, eventsContext, maxConcurrentStreams, stats,
                                                     purgeInterval = TimeSpan.FromHours 1, maxBytes = a.MaxBytes)
    match! args.Connect AppName with
    | (monitored, leases, (startFromTail, maxItems, tailSleepInterval, _lagFrequency), store) -> // normal case - consume from change feed, write to store
        let parseFeedDoc, sink =
            match args.Action with
            | Args.Action.Index _ -> index store
            | Args.Action.Snapshot _ -> snapshot store
            | Args.Action.Sync a -> sync a
        let source =
            Propulsion.CosmosStore.CosmosStoreSource(
                Log.Logger, args.StatsInterval, monitored, leases, processorName, parseFeedDoc, sink,
                startFromTail = startFromTail, ?maxItems = maxItems, tailSleepInterval = tailSleepInterval, ?lagEstimationInterval = args.LagEstimationInterval
            ).Start()
        return sink, source }

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

(*
// A typical app will likely have health checks etc, implying the wireup would be via `endpoints.MapMetrics()` and thus not use this ugly code directly
let startMetricsServer port: IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() } *)

let eofSignalException = System.Threading.Tasks.TaskCanceledException "Stopping; FeedMonitor wait completed"
let isExpectedShutdownSignalException: exn -> bool = function
    | :? Argu.ArguParseException // Via Arguments.Parse and/or Configuration.tryGet
    | :? System.Threading.Tasks.TaskCanceledException -> true // via AwaitKeyboardInterruptAsTaskCanceledException
    | _ -> false

let run args = async {
    let! sink, source = build args
    // use _metricsServer: IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    try do! [|  async { match args.WaitForTail with
                        | None -> ()
                        | Some initialWait ->
                            do! source.Monitor.AwaitCompletion(initialWait, awaitFullyCaughtUp = true, logInterval = args.StatsInterval / 2.) |> Async.AwaitTask
                            source.Stop()
                        do! source.AwaitWithStopOnCancellation() // Wait until Source has emitted stats
                        return raise eofSignalException } // trigger tear down of sibling waits
                sink.AwaitWithStopOnCancellation()
                Async.AwaitKeyboardInterruptAsTaskCanceledException() |] |> Async.Parallel |> Async.Ignore<unit[]>
    finally source.Flush() |> Async.Ignore |> Async.RunSynchronously } // flush checkpoints // TODO do! in F# 7

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try let metrics = Sinks.equinoxAndPropulsionConsumerMetrics (Sinks.tags AppName) args.ProcessorName
            Log.Logger <- LoggerConfiguration().Configure(args.Verbose).Sinks(metrics, args.CosmosVerbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with
            | :? Propulsion.Streams.HealthCheckException as e ->
                Log.Fatal(e, "Exiting due to Healthcheck; Stuck streams {stuck} Failing streams {failing}", e.StuckStreams, e.FailingStreams); 3
            | e when not (isExpectedShutdownSignalException e) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with x when x = eofSignalException -> printfn "Processing COMPLETE"; 0
        | :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"Exception %s{e.Message}"; 1
