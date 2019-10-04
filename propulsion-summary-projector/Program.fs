module ProjectorTemplate.Program

open Equinox.Cosmos
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

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<MainCommand; ExactlyOnce>]      ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-v"; Unique>]   Verbose
        | [<AltCommandLine "-vc"; Unique>]  VerboseConsole
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique; Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 64."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024."
                | Verbose ->                "request Verbose Logging. Default: off."
                | VerboseConsole ->         "request Verbose Console Logging. Default: off."
                | SrcCosmos _ ->            "specify CosmosDB input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.Verbose =                 a.Contains Parameters.Verbose
        member __.VerboseConsole =          a.Contains VerboseConsole
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead,64)
        member __.MaxConcurrentStreams =    a.GetResult(MaxWriters,1024)
        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member val Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (SrcCosmos cosmos) -> CosmosSourceArguments cosmos
            | _ -> raise (MissingArg "Must specify one of cosmos for Src")
        member x.SourceParams() =
                let srcC = x.Source
                let disco, auxColl =
                    match srcC.LeaseContainer with
                    | None ->     srcC.Discovery, { database = srcC.Database; container = srcC.Container + "-aux" }
                    | Some sc ->  srcC.Discovery, { database = srcC.Database; container = sc }
                Log.Information("Max read backlog: {maxReadAhead}", x.MaxReadAhead)
                Log.Information("Processing Lease {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                    x.ConsumerGroupName, auxColl.database, auxColl.container, srcC.MaxDocuments)
                if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
                srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
                srcC,(disco, auxColl, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency)
     and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-z"; Unique>]   FromTail
        | [<AltCommandLine "-m"; Unique>]   MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-cm">]          ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Unique>]   Container of string // Actually Mandatory, but stating that is not supported
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of int

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited."
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: off."
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. Default: envvar:EQUINOX_COSMOS_CONNECTION."
                | Database _ ->             "specify a database name for Cosmos account. Default: envvar:EQUINOX_COSMOS_DATABASE."
                | Container _ ->            "specify a container name within `Database`."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | Kafka _ ->                "Kafka Sink parameters."
    and CosmosSourceArguments(a : ParseResults<CosmosSourceParameters>) =
        member __.FromTail =                a.Contains CosmosSourceParameters.FromTail
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.LeaseContainer =          a.TryGetResult CosmosSourceParameters.LeaseContainer

        member __.Mode =                    a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Discovery.FromConnectionString __.Connection
        member __.Connection =              match a.TryGetResult CosmosSourceParameters.Connection with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =                match a.TryGetResult CosmosSourceParameters.Database   with Some x -> x | None -> envBackstop "Database" "EQUINOX_COSMOS_DATABASE"
        member __.Container =               a.GetResult CosmosSourceParameters.Container
        member __.Timeout =                 a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSourceParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5)
        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri,_)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, connector
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Kafka cosmos) -> KafkaSinkArguments cosmos
            | _ -> raise (MissingArg "Must specify `kafka` arguments")
    and [<NoEquality; NoComparison>] KafkaSinkParameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. Default: use environment variable PROPULSION_KAFKA_BROKER."
                | Topic _ ->                "specify Kafka Topic Id. Default: use environment variable PROPULSION_KAFKA_TOPIC"
    and KafkaSinkArguments(a : ParseResults<KafkaSinkParameters>) =
        member __.Broker =                  Uri(match a.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "PROPULSION_KAFKA_BROKER")
        member __.Topic =                       match a.TryGetResult Topic  with Some x -> x | None -> envBackstop "Topic"  "PROPULSION_KAFKA_TOPIC"
        member x.BuildTargetParams() =      x.Broker, x.Topic

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

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
        Log.Logger

let [<Literal>] appName = "ProjectorTemplate"

let build (args : CmdParser.Arguments) =
    let log = Logging.initialize args.Verbose args.VerboseConsole
    let (srcC,(auxDiscovery, aux, leaseId, startFromTail, maxDocuments, lagFrequency)) = args.SourceParams()
    let (discovery,cosmos,connector),(broker,topic) =
        srcC.BuildConnectionDetails(),srcC.Sink.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, appName, broker, topic)
    let produce (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
        producer.ProduceAsync(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)
    let cache = Equinox.Cosmos.Caching.Cache(appName, sizeMb = 10)
    let connection = connector.Connect(appName, discovery) |> Async.RunSynchronously
    let context = Equinox.Cosmos.Context(connection, cosmos.database, cosmos.container)
    let handleStreamEvents : (string*Propulsion.Streams.StreamSpan<_>) -> Async<int64> =
        let service = Todo.Repository.createService cache context
        Producer.handleAccumulatedEvents service produce
    let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
        docs |> Seq.collect EquinoxCosmosParser.enumStreamEvents
    let sink =
         Propulsion.Streams.Sync.StreamsSync.Start(
             log, args.MaxReadAhead, args.MaxConcurrentStreams, handleStreamEvents, category,
             statsInterval=TimeSpan.FromMinutes 1., dumpExternalStats=producer.DumpStats)
    let createObserver () = CosmosSource.CreateObserver(log, sink.StartIngester, mapToStreamItems)
    sink,CosmosSource.Run(log, discovery, connector.ClientOptions, cosmos,
        aux, leaseId, startFromTail, createObserver,
        ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency, auxDiscovery=auxDiscovery)

/// Handles command line parsing and running the program loop
// NOTE Any custom logic should go in main
let run args =
    try let projector,runSourcePipeline = args |> CmdParser.parse |> build
        runSourcePipeline |> Async.Start
        projector.AwaitCompletion() |> Async.RunSynchronously
        if projector.RanToCompletion then 0 else 2
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1

[<EntryPoint>]
let main argv =
    // TODO add any custom logic preprocessing commandline arguments and/or gathering custom defaults from external sources, etc
    try run argv
    // need to ensure all logs are flushed prior to exit
    finally Log.CloseAndFlush()