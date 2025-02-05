module ConsumerTemplate.Program

open Serilog
open System

type Configuration(tryGet) =

    let get key = match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"
    member _.Broker =                       get "PROPULSION_KAFKA_BROKER"
    member _.Topic =                        get "PROPULSION_KAFKA_TOPIC"
    member _.Group =                        get "PROPULSION_KAFKA_GROUP"

module Args =

    open Argu
    [<NoEquality; NoComparison; RequireSubcommand>]
    type Parameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        | [<AltCommandLine "-g"; Unique>]   Group of string
        | [<AltCommandLine "-m"; Unique>]   MaxInflightMb of float
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float

        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<CliPrefix(CliPrefix.None)>]     Cosmos of ParseResults<CosmosParameters>

        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic name. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
                | Group _ ->                "specify Kafka Consumer Group Id. (optional if environment variable PROPULSION_KAFKA_GROUP specified)"
                | MaxInflightMb _ ->        "maximum MiB of data to read ahead. Default: 10."
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off."

                | MaxWriters _ ->           "maximum number of items to process in parallel. Default: 8"
                | Verbose ->                "request verbose logging."
                | Cosmos _ ->               "specify CosmosDb input parameters"
    and Arguments(c: Configuration, p: ParseResults<Parameters>) =
        member val Cosmos =                 CosmosArguments(c, p.GetResult Cosmos)
        member val Broker =                 p.GetResult(Broker, fun () -> c.Broker)
        member val Topic =                  p.GetResult(Topic, fun () -> c.Topic)
        member val Group =                  p.GetResult(Group, fun () -> c.Group)
        member val MaxInFlightBytes =       p.GetResult(MaxInflightMb, 10.) * 1024. * 1024. |> int64
        member val LagFrequency =           p.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes

        member val MaxConcurrentStreams =   p.GetResult(MaxWriters, 8)

        member val Verbose =                p.Contains Verbose
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosArguments(c: Configuration, p: ParseResults<CosmosParameters>) =
        let discovery =                     p.GetResult(Connection, fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.GetResult(Database, fun () -> c.CosmosDatabase)
        let container =                     p.GetResult Container
        member _.Connect() =                connector.ConnectContext("Main", database, container, 256)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ConsumerTemplate"

let start (args: Args.Arguments) =
    let service =
        let store =
            let context = args.Cosmos.Connect() |> Async.RunSynchronously
            let cache = Equinox.Cache(AppName, sizeMb = 10)
            Store.Config.Cosmos (context, cache)
        TodoSummary.Factory.create store
    let config =
        FsKafka.KafkaConsumerConfig.Create(
            AppName, args.Broker, [args.Topic], args.Group, Confluent.Kafka.AutoOffsetReset.Earliest,
            maxInFlightBytes = args.MaxInFlightBytes, ?statisticsInterval = args.LagFrequency)
    let parseStreamSummaries(res: Confluent.Kafka.ConsumeResult<_, _>): seq<Propulsion.Streams.StreamEvent<_>> =
        Propulsion.Codec.NewtonsoftJson.RenderedSummary.parse res.Message.Value
    let stats = Ingester.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
    Propulsion.Kafka.Factory.StartConcurrent(Log.Logger, config, parseStreamSummaries, args.MaxConcurrentStreams, Ingester.ingest service, stats)

let run args = async {
    use consumer = start args
    return! consumer.AwaitWithStopOnCancellation()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintf $"Exception %s{e.Message}"; 1
