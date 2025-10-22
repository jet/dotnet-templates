module FeedConsumerTemplate.Program

open Serilog
open System

let [<Literal>] CONNECTION =                "EQUINOX_COSMOS_CONNECTION"
let [<Literal>] DATABASE =                  "EQUINOX_COSMOS_DATABASE"
let [<Literal>] CONTAINER =                 "EQUINOX_COSMOS_CONTAINER"

type Configuration(tryGet) =

    let get key = match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get CONNECTION
    member _.CosmosDatabase =               get DATABASE
    member _.CosmosContainer =              get CONTAINER

    member _.BaseUri =                      get "API_BASE_URI"
    member _.Group =                        get "API_CONSUMER_GROUP"

module Args =

    open Argu
    [<NoEquality; NoComparison; RequireSubcommand>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose

        | [<AltCommandLine "-g"; Unique>]   Group of string
        | [<AltCommandLine "-s"; Unique>]   SourceId of string
        | [<AltCommandLine "-f"; Unique>]   BaseUri of string

        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   FcsDop of int
        | [<AltCommandLine "-t"; Unique>]   TicketsDop of int

        | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request verbose logging."
                | Group _ ->                "specify Api Consumer Group Id. (optional if environment variable API_CONSUMER_GROUP specified)"
                | SourceId _ ->             "specify Api SourceId. Default: 'default'"
                | BaseUri _ ->              "specify Api endpoint. (optional if environment variable API_BASE_URI specified)"
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 8."
                | FcsDop _ ->               "maximum number of FCs to process in parallel. Default: 4"
                | TicketsDop _ ->           "maximum number of Tickets to process in parallel (per FC). Default: 4"
                | Cosmos _ ->               "Cosmos Store parameters."
    and Arguments(c: Configuration, p: ParseResults<Parameters>) =
        member val Verbose =                p.Contains Parameters.Verbose
        member val GroupId =                p.GetResult(Group, fun () -> c.Group)
        member val SourceId =               p.GetResult(SourceId, "default") |> Propulsion.Feed.SourceId.parse
        member val BaseUri =                p.GetResult(BaseUri, fun () -> c.BaseUri) |> Uri
        member val MaxReadAhead =           p.GetResult(MaxReadAhead, 8)
        member val FcsDop =                 p.GetResult(FcsDop, 4)
        member val TicketsDop =             p.GetResult(TicketsDop, 4)
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val CheckpointInterval =     TimeSpan.FromHours 1.
        member val TailSleepInterval =      TimeSpan.FromSeconds 1.
        member val Cosmos: CosmosArguments =
            match p.GetSubCommand() with
            | Cosmos cosmos -> CosmosArguments(c, cosmos)
            | _ -> p.Raise "Must specify cosmos"
    and [<NoEquality; NoComparison; RequireSubcommand>] CosmosParameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-cm">]          ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request verbose logging."
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           $"specify a connection string for a Cosmos account. (optional if environment variable $%s{CONNECTION} specified)"
                | Database _ ->             $"specify a database name for store. (optional if environment variable $%s{DATABASE} specified)"
                | Container _ ->            $"specify a container name for store. (optional if environment variable $%s{CONTAINER} specified)"
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
        member _.Connect(maxEvents) =       connector.ConnectContext("Main", database, container, maxEvents)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "FeedConsumerTemplate"

let build (args: Args.Arguments) =
    let cache = Equinox.Cache(AppName, sizeMb = 10)
    let context = args.Cosmos.Connect(maxEvents = 256) |> Async.RunSynchronously

    let sink =
        let stats = Ingester.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
        let handle = Ingester.handle args.TicketsDop
        Ingester.Factory.StartSink(Log.Logger, stats, args.FcsDop, handle, args.MaxReadAhead)
    let source =
        let checkpoints = Propulsion.Feed.ReaderCheckpoint.CosmosStore.create Store.Metrics.log (args.GroupId, args.CheckpointInterval) (context, cache)
        let feed = ApiClient.TicketsFeed args.BaseUri
        let source =
            Propulsion.Feed.FeedSource(
                Log.Logger, args.StatsInterval, args.SourceId, args.TailSleepInterval,
                checkpoints, sink)
        source.Start(feed.ReadTranches, fun t p -> feed.Poll(t, p))
    sink, source

let run args = async {
    let sink, source = build args
    use _ = sink
    use _ = source
    do! Async.Parallel [ source.AwaitWithStopOnCancellation(); sink.AwaitWithStopOnCancellation() ] |> Async.Ignore<unit[]>
    return if sink.RanToCompletion then 0 else 3
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try let metrics = Sinks.equinoxAndPropulsionFeedConsumerMetrics (Sinks.tags AppName) args.SourceId
            Log.Logger <- LoggerConfiguration().Configure(args.Verbose).Sinks(metrics, args.Cosmos.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously
            with e when not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2 
        finally Log.CloseAndFlush()
    with :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintf $"Exception %s{e.Message}"; 1
