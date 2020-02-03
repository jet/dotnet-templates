module ProjectorTemplate.Program

open Propulsion.Cosmos
open Propulsion.EventStore
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
    open Equinox.Cosmos
    open Equinox.EventStore
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-C"; Unique>]   VerboseConsole
        | [<CliPrefix(CliPrefix.None); AltCommandLine "es"; Unique(*ExactlyOnce is not supported*); Last>] SrcEs of ParseResults<EsSourceParameters>
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique; Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 64."
                | Verbose ->                "request Verbose Logging. Default: off."
                | VerboseConsole ->         "request Verbose Console Logging. Default: off."
                | SrcCosmos _ ->            "specify CosmosDB input parameters."
                | SrcEs _ ->                "specify EventStore input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.Verbose =                 a.Contains Parameters.Verbose
        member __.VerboseConsole =          a.Contains VerboseConsole
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead, 16)
        member __.MaxConcurrentStreams =    a.GetResult(MaxWriters, 64)
        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member val Source : Choice<EsSourceArguments, CosmosSourceArguments> =
            match a.TryGetSubCommand() with
            | Some (SrcEs es) -> Choice1Of2 (EsSourceArguments es)
            | Some (SrcCosmos cosmos) -> Choice2Of2 (CosmosSourceArguments cosmos)
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")
        member x.SourceParams() : Choice<EsSourceArguments*CosmosArguments*ReaderSpec, CosmosSourceArguments*_> =
            match x.Source with
            | Choice1Of2 srcE ->
                let startPos, cosmos = srcE.StartPos, srcE.CheckpointStore
                Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Container {container}",
                    x.ConsumerGroupName, startPos, srcE.ForceRestart, cosmos.Database, cosmos.Container)
                Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                    srcE.MinBatchSize, srcE.StartingBatchSize, x.MaxReadAhead)
                Choice1Of2 (srcE, cosmos,
                    {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                        forceRestart = srcE.ForceRestart
                        batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = 0 })
            | Choice2Of2 srcC ->
                let disco, auxColl =
                    match srcC.LeaseContainer with
                    | None ->     srcC.Discovery, { database = srcC.Database; container = srcC.Container + "-aux" }
                    | Some sc ->  srcC.Discovery, { database = srcC.Database; container = sc }
                Log.Information("Max read backlog: {maxReadAhead}", x.MaxReadAhead)
                Log.Information("Processing Lease {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                    x.ConsumerGroupName, auxColl.database, auxColl.container, srcC.MaxDocuments)
                if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
                srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
                Choice2Of2 (srcC, (disco, auxColl, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency))
    and [<NoEquality; NoComparison>] EsSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-g"; Unique>]   Gorge of int
        | [<AltCommandLine "-t"; Unique>]   Tail of intervalS: float
        | [<AltCommandLine "--force"; Unique>] ForceRestart
        | [<AltCommandLine "-m"; Unique>]   BatchSize of int
        | [<AltCommandLine "-mim"; Unique>] MinBatchSize of int
        | [<AltCommandLine "-pos"; Unique>] Position of int64
        | [<AltCommandLine "-c"; Unique>]   Chunk of int
        | [<AltCommandLine "-pct"; Unique>] Percent of float

        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-oh">]          HeartbeatTimeout of float
        | [<AltCommandLine "-h">]           Host of string
        | [<AltCommandLine "-x">]           Port of int
        | [<AltCommandLine "-u">]           Username of string
        | [<AltCommandLine "-p">]           Password of string

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "Start the processing from the Tail"
                | Gorge _ ->                "Request Parallel readers phase during initial catchup, running one chunk (256MB) apart. Default: off"
                | Tail _ ->                 "attempt to read from tail at specified interval in Seconds. Default: 1"
                | ForceRestart _ ->         "Forget the current committed position; start from (and commit) specified position. Default: start from specified position or resume from committed."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | MinBatchSize _ ->         "minimum item count to drop down to in reaction to read failures. Default: 512"
                | Position _ ->             "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"

                | Verbose ->                "Include low level Store logging."
                | Host _ ->                 "specify a DNS query, using Gossip-driven discovery against all A records returned. (optional if environment variable EQUINOX_ES_HOST specified)"
                | Port _ ->                 "specify a custom port. Defaults: envvar:EQUINOX_ES_PORT, 30778."
                | Username _ ->             "specify a username. (optional if environment variable EQUINOX_ES_USERNAME specified)"
                | Password _ ->             "specify a Password. (optional if environment variable EQUINOX_ES_PASSWORD specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds. Default: 1.5."

                | Cosmos _ ->               "CosmosDb Checkpoint Store parameters."
    and EsSourceArguments(a : ParseResults<EsSourceParameters>) =
        member __.Gorge =                   a.TryGetResult Gorge
        member __.TailInterval =            a.GetResult(Tail, 1.) |> TimeSpan.FromSeconds
        member __.ForceRestart =            a.Contains ForceRestart
        member __.StartingBatchSize =       a.GetResult(BatchSize, 4096)
        member __.MinBatchSize =            a.GetResult(MinBatchSize, 512)
        member __.StartPos =
            match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent, a.Contains EsSourceParameters.FromTail with
            | Some p, _, _, _ ->            Absolute p
            | _, Some c, _, _ ->            StartPos.Chunk c
            | _, _, Some p, _ ->            Percentage p
            | None, None, None, true ->     StartPos.TailOrCheckpoint
            | None, None, None, _ ->        StartPos.StartOrCheckpoint

        member __.Discovery =               match __.Port with Some p -> Discovery.GossipDnsCustomPort (__.Host, p) | None -> Discovery.GossipDns __.Host
        member __.Port =                    match a.TryGetResult Port with Some x -> Some x | None -> EnvVar.tryGet "EQUINOX_ES_PORT" |> Option.map int
        member __.Host =                    a.TryGetResult Host     |> defaultWithEnvVar "EQUINOX_ES_HOST"     "Host"
        member __.User =                    a.TryGetResult Username |> defaultWithEnvVar "EQUINOX_ES_USERNAME" "Username"
        member __.Password =                a.TryGetResult Password |> defaultWithEnvVar "EQUINOX_ES_PASSWORD" "Password"
        member __.Retries =                 a.GetResult(EsSourceParameters.Retries, 3)
        member __.Timeout =                 a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member __.Heartbeat =               a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
        member x.Connect(log: ILogger, storeLog: ILogger, appName, connectionStrategy) =
            let s (x : TimeSpan) = x.TotalSeconds
            let discovery = x.Discovery
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, s x.Heartbeat, s x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Establish(appName, discovery, connectionStrategy) |> Async.RunSynchronously

        member __.CheckpointInterval =  TimeSpan.FromHours 1.
        member val CheckpointStore : CosmosArguments =
            match a.TryGetSubCommand() with
            | Some (EsSourceParameters.Cosmos cosmos) -> CosmosArguments cosmos
            | _ -> raise (MissingArg "Must specify `cosmos` checkpoint store source is `es`")
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-m">]           ConnectionMode of ConnectionMode
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
                | Kafka _ ->                "Kafka Sink parameters."
    and CosmosArguments(a : ParseResults<CosmosParameters>) =
        member __.Mode =                    a.GetResult(CosmosParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Connection =              a.TryGetResult CosmosParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.TryGetResult CosmosParameters.Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member __.Timeout =                 a.GetResult(CosmosParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri, _) as discovery) = Discovery.FromConnectionString x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, connector
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (CosmosParameters.Kafka kafka) -> KafkaSinkArguments kafka
            | _ -> raise (MissingArg "Must specify `kafka` arguments")
     and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-md"; Unique>]  MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Unique>]   Container of string // Actually Mandatory, but stating that is not supported
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited."
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: off."
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name within `Database`."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | Kafka _ ->                "Kafka Sink parameters."
    and CosmosSourceArguments(a : ParseResults<CosmosSourceParameters>) =
        member __.FromTail =                a.Contains FromTail
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.LeaseContainer =          a.TryGetResult LeaseContainer

        member __.Mode =                    a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.GetResult Container
        member __.Timeout =                 a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri, _)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, connector
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (Kafka kafka) -> KafkaSinkArguments kafka
            | _ -> raise (MissingArg "Must specify `kafka` arguments")
     and [<NoEquality; NoComparison>] KafkaSinkParameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
    and KafkaSinkArguments(a : ParseResults<KafkaSinkParameters>) =
        member __.Broker =                  a.TryGetResult Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker" |> Uri
        member __.Topic =                   a.TryGetResult Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
        member x.BuildTargetParams() =      x.Broker, x.Topic

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

module Logging =

    let initialize verbose changeLogVerbose =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
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

let [<Literal>] appName = "ProjectorTemplate"

module EventStoreContext =
    let cache = Equinox.Cache(appName, sizeMb = 10)
    let create connection = Equinox.EventStore.Context(connection, Equinox.EventStore.BatchingPolicy(maxBatchSize=500))

let build (args : CmdParser.Arguments) =
    let src = args.SourceParams()
    let (discovery, cosmos, connector), (broker, topic) =
        match src with
        | Choice1Of2 (_srcE, cosmos, _spec) -> cosmos.BuildConnectionDetails(), cosmos.Sink.BuildTargetParams()
        | Choice2Of2 (srcC, _srcSpec) -> srcC.BuildConnectionDetails(), srcC.Sink.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, appName, broker, topic)
    let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
        producer.ProduceAsync(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)

    let cache = Equinox.Cache(appName, sizeMb = 10)
    let connection = connector.Connect(appName, discovery) |> Async.RunSynchronously
    let context = Equinox.Cosmos.Context(connection, cosmos.database, cosmos.container)

    match src with
    | Choice1Of2 (srcE, _cosmos, spec) ->
        let resolveCheckpointStream =
            let codec = FsCodec.NewtonsoftJson.Codec.Create()
            let caching = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
            let access = Equinox.Cosmos.AccessStrategy.Custom (Checkpoint.Fold.isOrigin, Checkpoint.Fold.transmute)
            fun target -> Equinox.Cosmos.Resolver(context, codec, Checkpoint.Fold.fold, Checkpoint.Fold.initial, caching, access).Resolve(target, Equinox.AllowStale)
        let checkpoints = Checkpoint.CheckpointSeries(spec.groupName, Log.ForContext<Checkpoint.CheckpointSeries>(), resolveCheckpointStream)
        let service =
            let connection = srcE.Connect(Log.Logger, Log.Logger, appName, Equinox.EventStore.ConnectionStrategy.ClusterSingle Equinox.EventStore.NodePreference.PreferSlave)
            let context = EventStoreContext.create connection
            Todo.EventStore.create (context, EventStoreContext.cache)
        let handle = Handler.handleEventStoreStreamEvents (service, produceSummary)

        let sink =
            Propulsion.Streams.Sync.StreamsSync.Start(
                 Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle,
                 statsInterval=TimeSpan.FromMinutes 1., dumpExternalStats=producer.DumpStats)
        let connect () = let c = srcE.Connect(Log.Logger, Log.Logger, appName, Equinox.EventStore.ConnectionStrategy.ClusterSingle Equinox.EventStore.NodePreference.PreferSlave) in c.ReadConnection
        let tryMapEvent (x : EventStore.ClientAPI.ResolvedEvent) =
            match x.Event with
            | e when not e.IsJson || e.EventStreamId.StartsWith "$" -> None
            | PropulsionStreamEvent e -> Some e
        sink, EventStoreSource.Run(
            Log.Logger, sink, checkpoints, connect, spec, tryMapEvent,
            args.MaxReadAhead, args.StatsInterval)
    | Choice2Of2 (_srcC, (auxDiscovery, aux, leaseId, startFromTail, maxDocuments, lagFrequency)) ->
        let service = Todo.Cosmos.create (context, cache)
        let handle = Handler.handleCosmosStreamEvents (service, produceSummary)

        let sink =
             Propulsion.Streams.Sync.StreamsSync.Start(
                 Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle,
                 statsInterval=TimeSpan.FromMinutes 1., dumpExternalStats=producer.DumpStats)
        let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
            docs |> Seq.collect EquinoxCosmosParser.enumStreamEvents
        let createObserver () = CosmosSource.CreateObserver(Log.Logger, sink.StartIngester, mapToStreamItems)
        sink, CosmosSource.Run(Log.Logger, connector.CreateClient(appName, discovery), cosmos,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency, auxClient=connector.CreateClient(appName, auxDiscovery))

let run argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose args.VerboseConsole
        Settings.initialize ()
        let projector, runSourcePipeline = build args
        runSourcePipeline |> Async.Start
        projector.AwaitCompletion() |> Async.RunSynchronously
        if projector.RanToCompletion then 0 else 2
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> Log.Fatal(e, "Exiting"); 1

[<EntryPoint>]
let main argv =
    try run argv
    finally Log.CloseAndFlush()