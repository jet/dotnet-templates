module ProjectorTemplate.Program

open Propulsion.Cosmos
open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj
    let set varName value : unit = Environment.SetEnvironmentVariable(varName, value)

module Settings =

    let private initEnvVar var key loadF =
        if None = EnvVar.tryGet var then
            printfn "Setting %s from %A" var key
            EnvVar.set var (loadF key)

    let initialize () =
        // e.g. initEnvVar     "EQUINOX_COSMOS_COLLECTION"    "CONSUL KEY" readFromConsul
        () // TODO add any custom logic preprocessing commandline arguments and/or gathering custom defaults from external sources, etc

module CmdParser =

    exception MissingArg of string
    let private getEnvVarForArgumentOrThrow varName argName =
        match EnvVar.tryGet varName with
        | None -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
        | Some x -> x
    let private defaultWithEnvVar varName argName = function
        | None -> getEnvVarForArgumentOrThrow varName argName
        | Some x -> x
    open Argu
    module Cosmos =
        open Equinox.Cosmos
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine "-cm">]      ConnectionMode of Equinox.Cosmos.ConnectionMode
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
                    | Connection _ ->       "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                    | Database _ ->         "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                    | Container _ ->        "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                    | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                    | Retries _ ->          "specify operation retries. Default: 1."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode =                a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.Direct)
            member __.Connection =          a.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
            member __.Database =            a.TryGetResult Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
            member __.Container =           a.TryGetResult Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"

            member __.Timeout =             a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Retries =             a.GetResult(Retries, 1)
            member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5)

            member x.BuildConnectionDetails() =
                let (Discovery.UriAndKey (endpointUri,_) as discovery) = Discovery.FromConnectionString x.Connection
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}.",
                    x.Mode, endpointUri, x.Database, x.Container)
                Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let connector = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
                discovery, { database = x.Database; container = x.Container }, connector.ClientOptions

    [<NoEquality; NoComparison>]
    type Parameters =
        (* ChangeFeed Args*)
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-as"; Unique>]  LeaseContainerSuffix of string
        | [<AltCommandLine "-z"; Unique>]   FromTail
        | [<AltCommandLine "-m"; Unique>]   MaxDocuments of int
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-v"; Unique>]   Verbose
        | [<AltCommandLine "-vc"; Unique>]  VerboseConsole
//#if kafka
        (* Kafka Args *)
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
//#endif
        (* Cosmos Source Args *)
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LeaseContainerSuffix _ ->  "specify Container Name suffix for Leases container. Default: `-aux`."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum document count to supply for the Change Feed query. Default: use response size limit"
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024"
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off"
                | Verbose ->                "request Verbose Logging. Default: off"
                | VerboseConsole ->         "request Verbose Logging from ChangeFeedProcessor. Default: off"
//#if kafka
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
//#endif
                | Cosmos _ ->               "specify CosmosDb input parameters"
    and Arguments(a : ParseResults<Parameters>) =
        member val Cosmos =                 Cosmos.Arguments(a.GetResult Cosmos)
//#if kafka
        member val Target =                 TargetInfo a
//#endif
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.Suffix =                   a.GetResult(LeaseContainerSuffix,"-aux")
        member __.Verbose =                 a.Contains Verbose
        member __.VerboseConsole =          a.Contains VerboseConsole
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead,64)
        member __.MaxConcurrentStreams =    a.GetResult(MaxWriters,1024)
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.AuxContainerName =        __.Cosmos.Container + __.Suffix
        member x.BuildChangeFeedParams() =
            match x.MaxDocuments with
            | None ->
                Log.Information("Processing {leaseId} in {auxContainerName} without document count limit (<= {maxPending} pending) using {dop} processors",
                    x.ConsumerGroupName, x.AuxContainerName, x.MaxReadAhead, x.MaxConcurrentStreams)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxContainerName} with max {changeFeedMaxDocuments} documents (<= {maxPending} pending) using {dop} processors",
                    x.ConsumerGroupName, x.AuxContainerName, lim, x.MaxReadAhead, x.MaxConcurrentStreams)
            if a.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            { database = x.Cosmos.Database; container = x.AuxContainerName }, x.ConsumerGroupName, a.Contains FromTail, x.MaxDocuments, x.LagFrequency,
            (x.MaxReadAhead, x.MaxConcurrentStreams)
//#if kafka
    and TargetInfo(a : ParseResults<Parameters>) =
        member __.Broker =                  a.TryGetResult Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker" |> Uri
        member __.Topic =                   a.TryGetResult Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
        member x.BuildTargetParams() = x.Broker, x.Topic
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

let replaceLongDataWithNull (x : FsCodec.ITimelineEvent<byte[]>) : FsCodec.ITimelineEvent<_> =
    if x.Data.Length < 900_000 then x
    else FsCodec.Core.TimelineEvent.Create(x.Index,x.EventType,null,x.Meta,timestamp=x.Timestamp) :> _

let hackDropBigBodies (e : Propulsion.Streams.StreamEvent<_>) : Propulsion.Streams.StreamEvent<_> =
    { stream = e.stream; event = replaceLongDataWithNull e.event }

let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
    docs
    |> Seq.collect EquinoxCosmosParser.enumStreamEvents
    // TODO use Seq.filter and/or Seq.map to adjust what's being sent etc
    // |> Seq.map hackDropBigBodies

#if kafka && nostreams
type ExampleOutput = { Id : string }
#endif

let [<Literal>] appName = "ProjectorTemplate"

let build (args : CmdParser.Arguments) =
    let discovery, source, clientOptions = args.Cosmos.BuildConnectionDetails()
    let aux, leaseId, startFromTail, maxDocuments, lagFrequency, (maxReadAhead, maxConcurrentStreams) = args.BuildChangeFeedParams()
#if kafka
    let (broker,topic) = args.Target.BuildTargetParams()
#if parallelOnly
    let render (doc : Microsoft.Azure.Documents.Document) : string * string =
        let equinoxPartition,documentId = doc.GetPropertyValue "p",doc.Id
        equinoxPartition,FsCodec.NewtonsoftJson.Serdes.Serialize { Id = documentId }
    let producer = Propulsion.Kafka.Producer(Log.Logger, appName, broker, topic)
    let projector =
        Propulsion.Kafka.ParallelProducerSink.Start(maxReadAhead, maxConcurrentStreams, render, producer, statsInterval=TimeSpan.FromMinutes 1.)
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, projector.StartIngester, fun x -> upcast x)
#else
    let render (stream: string, span: Propulsion.Streams.StreamSpan<_>) = async {
        return span
            |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
            |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize }
    let categorize (streamName : string) =
        streamName.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries).[0]
    let producer = Propulsion.Kafka.Producer(Log.Logger, appName, broker, topic)
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
    let sink =
        Propulsion.Streams.StreamsProjector.Start(
            Log.Logger, maxReadAhead, maxConcurrentStreams, project,
            categorize, statsInterval=TimeSpan.FromMinutes 1., stateInterval=TimeSpan.FromMinutes 5.)
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, sink.StartIngester, mapToStreamItems)
#endif
    let runSourcePipeline =
        CosmosSource.Run(
            Log.Logger, discovery, clientOptions, source,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
    sink,runSourcePipeline

let run argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose args.VerboseConsole
        Settings.initialize ()
        let sink,runSourcePipeline = build args
        runSourcePipeline |> Async.Start
        sink.AwaitCompletion() |> Async.RunSynchronously
        if sink.RanToCompletion then 0 else 2
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1

[<EntryPoint>]
let main argv =
    try run argv
    finally Log.CloseAndFlush()