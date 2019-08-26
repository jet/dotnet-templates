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
            | [<AltCommandLine "-s">] Connection of string
            | [<AltCommandLine "-cm">] ConnectionMode of ConnectionMode
            | [<AltCommandLine "-d">] Database of string
            | [<AltCommandLine "-c">] Container of string
            | [<AltCommandLine "-o">] Timeout of float
            | [<AltCommandLine "-r">] Retries of int
            | [<AltCommandLine "-rt">] RetriesWaitTime of int
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                    | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                    | Database _ ->         "specify a database name for Cosmos store (defaults: envvar:EQUINOX_COSMOS_DATABASE)."
                    | Container _ ->        " specify a container name for Cosmos store (defaults: envvar:EQUINOX_COSMOS_CONTAINER)."
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode = a.GetResult(ConnectionMode,ConnectionMode.Direct)
            member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Container =           match a.TryGetResult Container   with Some x -> x | None -> envBackstop "Container"  "EQUINOX_COSMOS_CONTAINER"

            member __.Timeout = a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Retries = a.GetResult(Retries, 1)
            member __.MaxRetryWaitTime = a.GetResult(RetriesWaitTime, 5)

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
        | [<AltCommandLine "-g"; Unique>] Group of string
        | [<AltCommandLine "-b"; Unique>] Broker of string
        | [<AltCommandLine "-t"; Unique>] Topic of string
        | [<AltCommandLine "-w"; Unique>] MaxDop of int
        | [<AltCommandLine "-m"; Unique>] MaxInflightGb of float
        | [<AltCommandLine "-l"; Unique>] LagFreqM of float
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>

        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Group _ ->            "specify Kafka Consumer Group Id. (optional if environment variable PROPULSION_KAFKA_GROUP specified)"
                | Broker _ ->           "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->            "specify Kafka Topic name. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
                | MaxDop _ ->           "maximum number of items to process in parallel. Default: 1024"
                | MaxInflightGb _ ->    "maximum GB of data to read ahead. Default: 0.5"
                | LagFreqM _ ->         "specify frequency (minutes) to dump lag stats. Default: off"
                | Verbose _ ->          "request verbose logging."
                | Cosmos _ ->           "specify CosmosDb input parameters"

    type Arguments(args : ParseResults<Parameters>) =
        member val Cosmos =             Cosmos.Arguments(args.GetResult Cosmos)
        member __.Broker =              Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "PROPULSION_KAFKA_BROKER")
        member __.Topic =               match args.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "PROPULSION_KAFKA_TOPIC"
        member __.Group =               match args.TryGetResult Group with Some x -> x | None -> envBackstop "Group" "PROPULSION_KAFKA_GROUP"
        member __.MaxDop =              match args.TryGetResult MaxDop with Some x -> x | None -> 1024
        member __.MaxInFlightBytes =    (match args.TryGetResult MaxInflightGb with Some x -> x | None -> 0.5) * 1024. * 1024. *1024. |> int64
        member __.LagFrequency =        args.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.Verbose =             args.Contains Verbose

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

module Repository =
    let codec = genCodec<Todo.Events.Event>()
    let cache = Caching.Cache ("ConsumerTemplate", 10)
    let resolve context =
        // We don't want to write any events, so here we supply the `transmute` function to teach it how to treat our events as snapshots
        let accessStrategy = AccessStrategy.RollingUnfolds (Todo.Folds.isOrigin,Todo.Folds.transmute)
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Resolver(context, codec, Todo.Folds.fold, Todo.Folds.initial, cacheStrategy, accessStrategy).Resolve

module Summary =

    /// A single Item in the Todo List
    type ItemInfo = { id: int; order: int; title: string; completed: bool }

    /// All data summarized for Summary Event Stream
    type SummaryInfo = { items : ItemInfo[] }

    /// Events we emit to third parties (kept here for ease of comparison, can be moved elsewhere in a larger app)
    type SummaryEvent =
        | Summary   of SummaryInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = genCodec<SummaryEvent>()

[<RequireQualifiedAccess>]
type Outcome = NoRelevantEvents of count : int | Ok of count : int | Skipped of count : int

type Stats(log, ?statsInterval, ?stateInterval) =
    inherit Propulsion.Kafka.StreamsConsumerStats<Outcome>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))

    let mutable (ok, na, redundant) = 0, 0, 0

    override __.HandleOk res = res |> function
        | Outcome.Ok count -> ok <- ok + 1; redundant <- redundant + count - 1
        | Outcome.Skipped count -> redundant <- redundant + count
        | Outcome.NoRelevantEvents count -> na <- na + count

    override __.DumpStats () =
        if ok <> 0 || na <> 0 || redundant <> 0 then
            log.Information(" Used {ok} Ignored {skipped} N/A {na}", ok, redundant, na)
            ok <- 0; na <- 0 ; redundant <- 0

let start (args : CmdParser.Arguments) =
    Logging.initialize args.Verbose
    let context = args.Cosmos.Connect("ProjectorTemplate") |> Async.RunSynchronously
    let service = Todo.Service(Log.ForContext<Todo.Service>(), Repository.resolve context)
    let (|ClientId|) (value : string) = ClientId.parse value
    let (|DecodeNewest|_|) (codec : Equinox.Codec.IUnionEncoder<_,_>) (stream, span : Propulsion.Streams.StreamSpan<_>) =
        StreamCodec.tryPickNewest Log.Logger codec (stream,span)
    let map : Summary.SummaryEvent -> Todo.Events.SummaryData = function
        | Summary.Summary x ->
            { items =
                [| for x in x.items ->
                    { id = x.id; order = x.order; title = x.title; completed = x.completed } |]}
    let ingestIncomingSummaryMessage (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<Outcome> = async {
        match stream, (stream,span) with
        | Category ("TodoSummary", ClientId clientId), DecodeNewest Summary.codec (version,summary) ->
            match! service.Ingest clientId (version,map summary) with
            | true -> return Outcome.Ok span.events.Length
            | false -> return Outcome.Skipped span.events.Length
        | _ -> return Outcome.NoRelevantEvents span.events.Length
    }
    let parseStreamSummaries(KeyValue (_streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
        Propulsion.Codec.NewtonsoftJson.RenderedSummary.parseStreamSummaries(spanJson)
    let config =
        Jet.ConfluentKafka.FSharp.KafkaConsumerConfig.Create(
            "ConsumerTemplate", args.Broker, [args.Topic], args.Group,
            maxInFlightBytes = args.MaxInFlightBytes, ?statisticsInterval = args.LagFrequency)
    let stats = Stats(Log.Logger)
    Propulsion.Kafka.StreamsConsumer.Start(Log.Logger, config, args.MaxDop, parseStreamSummaries, ingestIncomingSummaryMessage, stats, category)

[<EntryPoint>]
let main argv =
    try try use consumer = argv |> CmdParser.parse |> start
            Async.RunSynchronously <| consumer.AwaitCompletion()
            if consumer.RanToCompletion then 0 else 2
        with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
            | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
            // If the handler throws, we exit the app in order to let an orchesterator flag the failure
            | e -> Log.Fatal(e, "Exiting"); 1
    // need to ensure all logs are flushed prior to exit
    finally Log.CloseAndFlush()