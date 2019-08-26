﻿module ProjectorTemplate.Program

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

    module Cosmos =
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine "-s">] Connection of string
            | [<AltCommandLine "-cm">] ConnectionMode of Equinox.Cosmos.ConnectionMode
            | [<AltCommandLine "-d">] Database of string
            | [<AltCommandLine "-c">] Container of string
            | [<AltCommandLine "-o">] Timeout of float
            | [<AltCommandLine "-r">] Retries of int
            | [<AltCommandLine "-rt">] RetriesWaitTime of int
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                    | ConnectionMode _ ->   "override the connection mode (default: Direct)."
                    | Database _ ->         "specify a database name for store (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                    | Container _ ->        "specify a container name for store (defaults: envvar:EQUINOX_COSMOS_CONTAINER, test)."
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode =                a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.Direct)
            member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Container =           match a.TryGetResult Container   with Some x -> x | None -> envBackstop "Container"  "EQUINOX_COSMOS_CONTAINER"

            member __.Timeout = a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Retries = a.GetResult(Retries, 1)
            member __.MaxRetryWaitTime = a.GetResult(RetriesWaitTime, 5)

            member x.BuildConnectionDetails() =
                let (Discovery.UriAndKey (endpointUri,_) as discovery) = Discovery.FromConnectionString x.Connection
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}.",
                    x.Mode, endpointUri, x.Database, x.Container)
                Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let connector = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
                discovery, connector, { database = x.Database; container = x.Container }

    [<NoEquality; NoComparison>]
    type Parameters =
        (* ChangeFeed Args*)
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine "-as"; Unique>] LeaseContainerSuffix of string
        | [<AltCommandLine "-z"; Unique>] FromTail
        | [<AltCommandLine "-m"; Unique>] MaxDocuments of int
        | [<AltCommandLine "-r"; Unique>] MaxPendingBatches of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
        | [<AltCommandLine "-l"; Unique>] LagFreqM of float
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
//#if kafka
        (* Kafka Args *)
        | [<AltCommandLine "-b"; Unique>] Broker of string
        | [<AltCommandLine "-t"; Unique>] Topic of string
        | [<AltCommandLine "-p"; Unique>] Producers of int
//#endif
        (* Cosmos Source Args *)
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LeaseContainerSuffix _ -> "specify Container Name suffix for Leases container (default: `-aux`)."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum document count to supply for the Change Feed query. Default: use response size limit"
                | MaxPendingBatches _ ->    "maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024"
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off"
                | Verbose ->                "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
//#if kafka
                | Broker _ ->               "specify Kafka Broker, in host:port format. (default: use environment variable PROPULSION_KAFKA_BROKER, if specified)"
                | Topic _ ->                "specify Kafka Topic Id. (default: use environment variable PROPULSION_KAFKA_TOPIC, if specified)"
                | Producers _ ->            "specify number of Kafka Producer instances to use. Default: 1"
//#endif
                | Cosmos _ ->               "specify CosmosDb input parameters"
    and Arguments(args : ParseResults<Parameters>) =
        member val Cosmos = Cosmos.Arguments(args.GetResult Cosmos)
//#if kafka
        member val Target = TargetInfo args
//#endif
        member __.LeaseId =                 args.GetResult ConsumerGroupName
        member __.Suffix =                  args.GetResult(LeaseContainerSuffix,"-aux")
        member __.Verbose =                 args.Contains Verbose
        member __.ChangeFeedVerbose =       args.Contains ChangeFeedVerbose
        member __.MaxDocuments =            args.TryGetResult MaxDocuments
        member __.MaxReadAhead =            args.GetResult(MaxPendingBatches,64)
        member __.ConcurrentStreamProcessors = args.GetResult(MaxWriters,1024)
        member __.LagFrequency =            args.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.AuxContainerName =       __.Cosmos.Container + __.Suffix
        member x.BuildChangeFeedParams() =
            match x.MaxDocuments with
            | None ->
                Log.Information("Processing {leaseId} in {auxContainerName} without document count limit (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxContainerName, x.MaxReadAhead, x.ConcurrentStreamProcessors)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxContainerName} with max {changeFeedMaxDocuments} documents (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxContainerName, lim, x.MaxReadAhead, x.ConcurrentStreamProcessors)
            if args.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            { database = x.Cosmos.Database; container = x.AuxContainerName}, x.LeaseId, args.Contains FromTail, x.MaxDocuments, x.LagFrequency,
            (x.MaxReadAhead, x.ConcurrentStreamProcessors)
//#if kafka
    and TargetInfo(args : ParseResults<Parameters>) =
        member __.Broker    = Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "PROPULSION_KAFKA_BROKER")
        member __.Topic     =     match args.TryGetResult Topic  with Some x -> x | None -> envBackstop "Topic"  "PROPULSION_KAFKA_TOPIC"
        member __.Producers = args.GetResult(Producers,1)
        member x.BuildTargetParams() = x.Broker, x.Topic, x.Producers
//#endif

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

let replaceLongDataWithNull (x : Propulsion.Streams.IEvent<byte[]>) : Propulsion.Streams.IEvent<_> =
    //if x.Data.Length < 900_000 then x else
    Propulsion.Streams.Internal.EventData.Create(x.EventType,null,x.Meta,x.Timestamp) :> _

let hackDropBigBodies (e : Propulsion.Streams.StreamEvent<_>) : Propulsion.Streams.StreamEvent<_> =
    { stream = e.stream; index = e.index; event = replaceLongDataWithNull e.event }

let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
    docs
    |> Seq.collect EquinoxCosmosParser.enumStreamEvents
    // TODO use Seq.filter and/or Seq.map to adjust what's being sent
    |> Seq.map hackDropBigBodies

#if kafka && nostreams
type ExampleOutput = { Id : string }
#endif

let start (args : CmdParser.Arguments) =
    Logging.initialize args.Verbose args.ChangeFeedVerbose
    let discovery, connector, source = args.Cosmos.BuildConnectionDetails()
    let aux, leaseId, startFromTail, maxDocuments, lagFrequency, (maxReadAhead, maxConcurrentStreams) = args.BuildChangeFeedParams()
#if kafka
    let (broker,topic, producers) = args.Target.BuildTargetParams()
#if parallelOnly
    let render (doc : Microsoft.Azure.Documents.Document) : string * string =
        let equinoxPartition,documentId = doc.GetPropertyValue "p",doc.Id
        equinoxPartition,Newtonsoft.Json.JsonConvert.SerializeObject { Id = documentId }
    let producer = 
        Propulsion.Kafka.Producer(
            Log.Logger, "ProjectorTemplate", broker, topic, degreeOfParallelism = producers(*,
            customize = fun c -> c.CompressionLevel <- Nullable 0; c.CompressionType <- Nullable Confluent.Kafka.CompressionType.None*))
    let projector =
        Propulsion.Kafka.ParallelProducerSink.Start(maxReadAhead, maxConcurrentStreams, render, producer, statsInterval=TimeSpan.FromMinutes 1.)
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, projector.StartIngester, fun x -> upcast x)
#else
    let serializerSettings = Newtonsoft.Json.JsonSerializerSettings()
    let render (stream: string, span: Propulsion.Streams.StreamSpan<_>) =
        let rendered = Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream span
        Newtonsoft.Json.JsonConvert.SerializeObject(rendered, typeof<Propulsion.Codec.NewtonsoftJson.RenderedSpan>, serializerSettings)
    let categorize (streamName : string) =
        streamName.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries).[0]
    let producer = 
        Propulsion.Kafka.Producer(
            Log.Logger, "ProjectorTemplate", broker, topic, degreeOfParallelism = producers(*,
            customize = fun c -> c.CompressionLevel <- Nullable 0; c.CompressionType <- Nullable Confluent.Kafka.CompressionType.None*))
    let projector =
        Propulsion.Kafka.StreamsProducerSink.Start(
            Log.Logger, maxReadAhead, maxConcurrentStreams, render, producer,
            categorize, statsInterval=TimeSpan.FromMinutes 1., stateInterval=TimeSpan.FromMinutes 2.)
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, projector.StartIngester, mapToStreamItems)
#endif
#else
    let project (_stream, span: Propulsion.Streams.StreamSpan<_>) = async { 
        let r = Random()
        let ms = r.Next(1,span.events.Length)
        do! Async.Sleep ms }
    let categorize (streamName : string) = streamName.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries).[0]
    let projector =
        Propulsion.Streams.StreamsProjector.Start(
            Log.Logger, maxReadAhead, maxConcurrentStreams, project,
            categorize, statsInterval=TimeSpan.FromMinutes 1., stateInterval=TimeSpan.FromMinutes 5.)
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, projector.StartIngester, mapToStreamItems)
#endif
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