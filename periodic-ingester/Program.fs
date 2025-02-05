module PeriodicIngesterTemplate.Ingester.Program

open Serilog
open System

type Configuration(tryGet) =

    let get key = match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"

    member _.PrometheusPort =               tryGet "PROMETHEUS_PORT" |> Option.map int

    member _.BaseUri =                      get "API_BASE_URI"
    member _.Group =                        get "API_CONSUMER_GROUP"

module Args =

    open Argu

    type [<NoEquality; NoComparison; RequireSubcommand>] Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int
        | [<AltCommandLine "-g"; Mandatory>] GroupId of string

        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-t"; Unique>]   TicketsDop of int

        | [<CliPrefix(CliPrefix.None)>] Feed of ParseResults<FeedParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request verbose logging."
                | GroupId _ ->              "consumer group name. Default: 'default'"
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off (optional if environment variable PROMETHEUS_PORT specified)"
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 8."
                | TicketsDop _ ->           "maximum number of Tickets to process in parallel. Default: 4"
                | Feed _ ->                 "Feed parameters."
    and Arguments(c: Configuration, p: ParseResults<Parameters>) =
        member val GroupId =                p.GetResult(GroupId, "default")

        member val Verbose =                p.Contains Parameters.Verbose
        member val PrometheusPort =         p.TryGetResult PrometheusPort |> Option.orElseWith (fun () -> c.PrometheusPort)
        member val MaxReadAhead =           p.GetResult(MaxReadAhead, 8)
        member val TicketsDop =             p.GetResult(TicketsDop, 4)
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val CheckpointInterval =     TimeSpan.FromHours 1.
        member val Feed: FeedArguments =
            match p.GetSubCommand() with
            | Feed feed -> FeedArguments(c, feed)
            | _ -> p.Raise "Must specify feed"
    and [<NoEquality; NoComparison; RequireSubcommand>] FeedParameters =
        | [<AltCommandLine "-g"; Unique>]   Group of string
        | [<AltCommandLine "-f"; Unique>]   BaseUri of string
        | [<CliPrefix(CliPrefix.None); Unique>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Group _ ->                "specify Api Consumer Group Id. (optional if environment variable API_CONSUMER_GROUP specified)"
                | BaseUri _ ->              "specify Api endpoint. (optional if environment variable API_BASE_URI specified)"
                | Cosmos _ ->               "Cosmos Store parameters."
    and FeedArguments(c: Configuration, p: ParseResults<FeedParameters>) =
        member val SourceId =               p.GetResult(Group, fun () -> c.Group) |> Propulsion.Feed.SourceId.parse
        member val BaseUri =                p.GetResult(BaseUri, fun () -> c.BaseUri) |> Uri
        member val RefreshInterval =        TimeSpan.FromHours 1.
        member val Cosmos: CosmosArguments =
            match p.GetSubCommand() with
            | Cosmos cosmos -> CosmosArguments(c, cosmos)
            | _ -> p.Raise "unexpected"
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request verbose logging."
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Retries _ ->              "specify operation retries (default: 9)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 30)"
    and CosmosArguments(c: Configuration, p: ParseResults<CosmosParameters>) =
        let discovery =                     p.GetResult(Connection, fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let retries =                       p.GetResult(Retries, 9)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, retries, maxRetryWaitTime, ?mode=mode)
        let database =                      p.GetResult(Database, fun () -> c.CosmosDatabase)
        let container =                     p.GetResult(Container, fun () -> c.CosmosContainer)
        member val Verbose =                p.Contains Verbose
        member _.Connect() =                connector.ConnectContext("Main", database, container)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "PeriodicIngesterTemplate"

let build (args: Args.Arguments) =
    let cache = Equinox.Cache(AppName, sizeMb = 10)
    let feed = args.Feed
    let context = feed.Cosmos.Connect() |> Async.RunSynchronously

    let sink =
        let stats = Ingester.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
        Ingester.Factory.StartSink(Log.Logger, stats, args.TicketsDop, Ingester.handle, args.MaxReadAhead)
    let source =
        let checkpoints = Propulsion.Feed.ReaderCheckpoint.CosmosStore.create Log.Logger (args.GroupId, args.CheckpointInterval) (context, cache)
        let client = ApiClient.TicketsFeed feed.BaseUri
        let source =
            Propulsion.Feed.PeriodicSource(
                Log.Logger, args.StatsInterval, feed.SourceId,
                feed.RefreshInterval, checkpoints, sink)
        source.Start(client.Crawl)
    sink, source

// A typical app will likely have health checks etc, implying the wireup would be via `endpoints.MapMetrics()` and thus not use this ugly code directly
let startMetricsServer port: IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() }

let run args = async {
    let sink, source = build args
    use _ = source
    use _ = sink
    use _metricsServer: IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    return! Async.Parallel [ source.AwaitWithStopOnCancellation(); sink.AwaitWithStopOnCancellation() ] |> Async.Ignore<unit[]> }

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try let metrics = Sinks.equinoxAndPropulsionFeedConsumerMetrics (Sinks.tags AppName) args.Feed.SourceId
            Log.Logger <- LoggerConfiguration().Configure(args.Verbose).Sinks(metrics, args.Feed.Cosmos.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintf $"Exception %s{e.Message}"; 1
