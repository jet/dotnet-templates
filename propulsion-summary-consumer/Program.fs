module ConsumerTemplate.Program

open Equinox.Cosmos
open Serilog
open System

module CmdParser =
    open Argu

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argument or via the %s environment variable" msg key)
        | x -> x

    module Cosmos =
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine "-cm">]      ConnectionMode of ConnectionMode
            | [<AltCommandLine "-s">]       Connection of string
            | [<AltCommandLine "-d">]       Database of string
            | [<AltCommandLine "-c">]       Container of string
            | [<AltCommandLine "-o">]       Timeout of float
            | [<AltCommandLine "-r">]       Retries of int
            | [<AltCommandLine "-rt">]      RetriesWaitTime of int
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | ConnectionMode _ ->   "override the connection mode. Default: Direct."
                    | Connection _ ->       "specify a connection string for a Cosmos account. Default: envvar:EQUINOX_COSMOS_CONNECTION."
                    | Database _ ->         "specify a database name for Cosmos store. Default: envvar:EQUINOX_COSMOS_DATABASE."
                    | Container _ ->        " specify a container name for Cosmos store. Default: envvar:EQUINOX_COSMOS_CONTAINER."
                    | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                    | Retries _ ->          "specify operation retries. Default: 1."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode =                a.GetResult(ConnectionMode,ConnectionMode.Direct)
            member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Container =           match a.TryGetResult Container   with Some x -> x | None -> envBackstop "Container"  "EQUINOX_COSMOS_CONTAINER"

            member __.Timeout =             a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Retries =             a.GetResult(Retries, 1)
            member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5)

            member x.Connect(clientId) = async {
                let (Discovery.UriAndKey (endpointUri,_) as discovery) = Discovery.FromConnectionString x.Connection
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}.",
                    x.Mode, endpointUri, x.Database, x.Container)
                Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let! connection = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode).Connect(clientId,discovery)
                return Context(connection, x.Database, x.Container) }

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-g"; Unique>]   Group of string
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        | [<AltCommandLine "-w"; Unique>]   MaxDop of int
        | [<AltCommandLine "-m"; Unique>]   MaxInflightGb of float
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-v"; Unique>]   Verbose
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>

        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Group _ ->                "specify Kafka Consumer Group Id. (optional if environment variable PROPULSION_KAFKA_GROUP specified)"
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic name. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
                | MaxDop _ ->               "maximum number of items to process in parallel. Default: 1024"
                | MaxInflightGb _ ->        "maximum GB of data to read ahead. Default: 0.5"
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off"
                | Verbose _ ->              "request verbose logging."
                | Cosmos _ ->               "specify CosmosDb input parameters"

    type Arguments(args : ParseResults<Parameters>) =
        member val Cosmos =                 Cosmos.Arguments(args.GetResult Cosmos)
        member __.Broker =                  Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "PROPULSION_KAFKA_BROKER")
        member __.Topic =                   match args.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "PROPULSION_KAFKA_TOPIC"
        member __.Group =                   match args.TryGetResult Group with Some x -> x | None -> envBackstop "Group" "PROPULSION_KAFKA_GROUP"
        member __.MaxDop =                  match args.TryGetResult MaxDop with Some x -> x | None -> 1024
        member __.MaxInFlightBytes =        (match args.TryGetResult MaxInflightGb with Some x -> x | None -> 0.5) * 1024. * 1024. *1024. |> int64
        member __.LagFrequency =            args.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.Verbose =                 args.Contains Verbose

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

module Logging =
    let initialize verbose =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                        if not verbose then c.WriteTo.Console(theme=theme)
                        else c.WriteTo.Console(theme=theme, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}|{Properties}{NewLine}{Exception}")
            |> fun c -> c.CreateLogger()

let [<Literal>] appName = "ConsumerTemplate"

let startConsumer (args : CmdParser.Arguments) =
    Logging.initialize args.Verbose
    let context = args.Cosmos.Connect(appName) |> Async.RunSynchronously
    let cache = Caching.Cache (appName, 10) // here rather than in Todo aggregate as it can be shared with other Aggregates
    let service = TodoSummary.Repository.createService cache context
    let config =
        Jet.ConfluentKafka.FSharp.KafkaConsumerConfig.Create(
            appName, args.Broker, [args.Topic], args.Group,
            maxInFlightBytes = args.MaxInFlightBytes, ?statisticsInterval = args.LagFrequency)
    SummaryIngester.startConsumer config Log.Logger service args.MaxDop

[<EntryPoint>]
let main argv =
    try try use consumer = argv |> CmdParser.parse |> startConsumer
            Async.RunSynchronously <| consumer.AwaitCompletion()
            if consumer.RanToCompletion then 0 else 2
        with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
            | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
            // If the handler throws, we exit the app in order to let an orchestrator flag the failure
            | e -> Log.Fatal(e, "Exiting"); 1
    // need to ensure all logs are flushed prior to exit
    finally Log.CloseAndFlush()