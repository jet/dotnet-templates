module ProjectorTemplate.Program

open Equinox.Cosmos
open FSharp.UMX
open Propulsion.Codec.NewtonsoftJson
open Propulsion.Cosmos
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
            | [<AltCommandLine "-cm">] ConnectionMode of Equinox.Cosmos.ConnectionMode
            | [<AltCommandLine "-d">] Database of string
            | [<AltCommandLine "-c">] Collection of string
            | [<AltCommandLine "-o">] Timeout of float
            | [<AltCommandLine "-r">] Retries of int
            | [<AltCommandLine "-rt">] RetriesWaitTime of int
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                    | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                    | Database _ ->         "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                    | Collection _ ->       "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode = a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.Direct)
            member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

            member __.Timeout = a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Retries = a.GetResult(Retries, 1)
            member __.MaxRetryWaitTime = a.GetResult(RetriesWaitTime, 5)

            member x.BuildConnectionDetails() =
                let (Discovery.UriAndKey (endpointUri,_) as discovery) = Discovery.FromConnectionString x.Connection
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Collection {collection}.",
                    x.Mode, endpointUri, x.Database, x.Collection)
                Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let connector = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
                discovery, connector, { database = x.Database; collection = x.Collection }

    [<NoEquality; NoComparison>]
    type Parameters =
        (* ChangeFeed Args*)
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSuffix of string
        | [<AltCommandLine "-z"; Unique>] FromTail
        | [<AltCommandLine "-m"; Unique>] MaxDocuments of int
        | [<AltCommandLine "-r"; Unique>] MaxPendingBatches of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
        | [<AltCommandLine "-l"; Unique>] LagFreqM of float
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        (* Kafka Args *)
        | [<AltCommandLine "-b"; Unique>] Broker of string
        | [<AltCommandLine "-t"; Unique>] Topic of string
        | [<AltCommandLine "-p"; Unique>] Producers of int
        (* Cosmos Source Args *)
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LeaseCollectionSuffix _ -> "specify Collection Name suffix for Leases collection (default: `-aux`)."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum document count to supply for the Change Feed query. Default: use response size limit"
                | MaxPendingBatches _ ->    "maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024"
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off"
                | Verbose ->                "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | Broker _ ->               "specify Kafka Broker, in host:port format. (default: use environment variable PROPULSION_KAFKA_BROKER, if specified)"
                | Topic _ ->                "specify Kafka Topic Id. (default: use environment variable PROPULSION_KAFKA_TOPIC, if specified)"
                | Producers _ ->            "specify number of Kafka Producer instances to use. Default: 1"
                | Cosmos _ ->               "specify CosmosDb input parameters"
    and Arguments(args : ParseResults<Parameters>) =
        member val Cosmos = Cosmos.Arguments(args.GetResult Cosmos)
        member val Target = TargetInfo args
        member __.LeaseId =                 args.GetResult ConsumerGroupName
        member __.Suffix =                  args.GetResult(LeaseCollectionSuffix,"-aux")
        member __.Verbose =                 args.Contains Verbose
        member __.ChangeFeedVerbose =       args.Contains ChangeFeedVerbose
        member __.MaxDocuments =            args.TryGetResult MaxDocuments
        member __.MaxReadAhead =            args.GetResult(MaxPendingBatches,64)
        member __.ConcurrentStreamProcessors = args.GetResult(MaxWriters,1024)
        member __.LagFrequency =            args.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.AuxCollectionName =       __.Cosmos.Collection + __.Suffix
        member x.BuildChangeFeedParams() =
            match x.MaxDocuments with
            | None ->
                Log.Information("Processing {leaseId} in {auxCollName} without document count limit (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxCollectionName, x.MaxReadAhead, x.ConcurrentStreamProcessors)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxCollName} with max {changeFeedMaxDocuments} documents (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxCollectionName, lim, x.MaxReadAhead, x.ConcurrentStreamProcessors)
            if args.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            { database = x.Cosmos.Database; collection = x.AuxCollectionName}, x.LeaseId, args.Contains FromTail, x.MaxDocuments, x.LagFrequency,
            (x.MaxReadAhead, x.ConcurrentStreamProcessors)
    and TargetInfo(args : ParseResults<Parameters>) =
        member __.Broker = Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "PROPULSION_KAFKA_BROKER")
        member __.Topic = match args.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "PROPULSION_KAFKA_TOPIC"
        member __.Producers = args.GetResult(Producers,1)
        member x.BuildTargetParams() = x.Broker, x.Topic, x.Producers

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose changeLogVerbose =
        Log.Logger <-
            LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            // LibLog writes to the global logger, so we need to control the emission
            |> fun c -> let cfpl = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
                        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
                        if changeLogVerbose then c else c.Filter.ByExcluding(fun x -> isCfp x)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()

let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
    docs
    |> Seq.collect EquinoxCosmosParser.enumStreamEvents

module TodosRepository =
    /// Allows one to hook in any JsonConverters etc
    let serializationSettings = Newtonsoft.Json.JsonSerializerSettings()
    /// Automatically generates a Union Codec based using the scheme described in https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/
    let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.Codec.NewtonsoftJson.Json.Create<'Union>(serializationSettings)
    let codec = genCodec<Todo.Events.Event>()

    let cache = Caching.Cache ("ProjectorTemplate", 10)

    let resolve context =
        let accessStrategy = Equinox.Cosmos.AccessStrategy.Snapshot (Todo.Folds.isOrigin,Todo.Folds.compact)
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Equinox.Cosmos.Resolver(context, codec, Todo.Folds.fold, Todo.Folds.initial, cacheStrategy, accessStrategy).Resolve

let start (args : CmdParser.Arguments) =
    Logging.initialize args.Verbose args.ChangeFeedVerbose
    let discovery, connector, source = args.Cosmos.BuildConnectionDetails()
    let aux, leaseId, startFromTail, maxDocuments, lagFrequency, (maxReadAhead, maxConcurrentStreams) = args.BuildChangeFeedParams()
    let (broker,topic, producers) = args.Target.BuildTargetParams()
    let connection = Async.RunSynchronously <| connector.Connect("ProjectorTemplate",discovery)
    let context = Context(Gateway(connection, BatchingPolicy(defaultMaxItems=500)), Containers(source.database,source.collection))
    let service = Todo.Service(Log.ForContext<Todo.Service>(), TodosRepository.resolve context)
    let (|ClientId|) (value : string) = ClientId.parse value
    let summaryCodec = TodosRepository.genCodec<Todo.Summary.SummaryEvent>()
    let impliesSummaryUpdateNecessary = function
        | Todo.Events.Snapshot _ -> false
        | _ -> true
    let (|Decode|) (codec : Equinox.Codec.IUnionEncoder<_,_>) (span : Propulsion.Streams.StreamSpan<_>) =
        span.events |> Seq.map EventCodec.toCodecEvent |> Seq.choose codec.TryDecode
    let mapStreamChangesToKafkaMessage (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<string option> = async {
        match stream, span with
        | Category (Todo.categoryId, ClientId clientId), (Decode TodosRepository.codec events)
                when  events  |> Seq.exists impliesSummaryUpdateNecessary ->
            let! version,summary = Todo.Summary.summarize service clientId
            let rendered =
                summary
                |> summaryCodec.Encode
                |> EventCodec.toStreamEvent
                |> RenderedSummary.ofStreamEvent stream version
            return Some Newtonsoft.Json.JsonConvert.SerializeObject rendered
        | _ ->
            return None
    }
    let categorize = id
    let producer = Propulsion.Kafka.Producer(Log.Logger, "ProjectorTemplate", broker, topic, degreeOfParallelism = producers)
    let projector =
        Propulsion.Kafka.StreamsProducerSink.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, mapStreamChangesToKafkaMessage, producer, categorize, statsInterval=TimeSpan.FromMinutes 1.)
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, projector.StartIngester, mapToStreamItems)
    let runSourcePipeline =
        CosmosSource.Run(
            Log.Logger, discovery, connector.ClientOptions, source,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
    runSourcePipeline, projector

[<EntryPoint>]
let main argv =
    try try let args = CmdParser.parse argv
            let runSourcePipeline, projector = start args
            Async.Start <| runSourcePipeline
            Async.RunSynchronously <| projector.AwaitCompletion()
            if projector.RanToCompletion then 0 else 2
        with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
            | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
            | e -> eprintfn "%s" e.Message; 1
    finally Log.CloseAndFlush()