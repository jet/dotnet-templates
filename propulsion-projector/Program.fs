﻿module ProjectorTemplate.Program

#if cosmos
open Propulsion.Cosmos
#endif
open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj
    let set varName value : unit = Environment.SetEnvironmentVariable(varName, value)

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-configuration
// - this is where any custom retrieval of settings not arriving via commandline arguments or environment variables should go
// - values should be propagated by setting environment variables and/or returning them from `initialize`
module Configuration =

    let private initEnvVar var key loadF =
        if None = EnvVar.tryGet var then
            printfn "Setting %s from %A" var key
            EnvVar.set var (loadF key)

    let initialize () =
        // e.g. initEnvVar     "EQUINOX_COSMOS_CONTAINER"    "CONSUL KEY" readFromConsul
        () // TODO add any custom logic preprocessing commandline arguments and/or gathering custom defaults from external sources, etc

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-args
// - this module is responsible solely for parsing/validating the commandline arguments (including falling back to values supplied via environment variables)
// - It's expected that the properties on *Arguments types will summarize the active settings as a side effect of
// TODO DONT invest time reorganizing or reformatting this - half the value is having a legible summary of all program parameters in a consistent value
//      you may want to regenerate it at a different time and/or facilitate comparing it with the `module Args` of other programs
// TODO NEVER hack temporary overrides in here; if you're going to do that, use commandline arguments that fall back to environment variables
//      or (as a last resort) supply them via code in `module Configuration`
module Args =

    exception MissingArg of string
    let private getEnvVarForArgumentOrThrow varName argName =
        match EnvVar.tryGet varName with
        | None -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
        | Some x -> x
    let private defaultWithEnvVar varName argName = function
        | None -> getEnvVarForArgumentOrThrow varName argName
        | Some x -> x
//#if esdb
    let private isEnvVarTrue varName =
         EnvVar.tryGet varName |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))
//#endif
    let private seconds (x : TimeSpan) = x.TotalSeconds
    open Argu
#if cosmos
    type [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose
        | [<AltCommandLine "-as"; Unique>]  LeaseContainerSuffix of string
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-md"; Unique>]  MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float

        | [<AltCommandLine "-m">]       ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]       Connection of string
        | [<AltCommandLine "-d">]       Database of string
        | [<AltCommandLine "-c">]       Container of string
        | [<AltCommandLine "-o">]       Timeout of float
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-rt">]      RetriesWaitTime of float
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | CfpVerbose ->         "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | LeaseContainerSuffix _ -> "specify Container Name suffix for Leases container. Default: `-aux`."
                | FromTail _ ->         "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->     "maximum document count to supply for the Change Feed query. Default: use response size limit"
                | LagFreqM _ ->         "specify frequency (minutes) to dump lag stats. Default: off"

                | ConnectionMode _ ->   "override the connection mode. Default: Direct."
                | Connection _ ->       "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->         "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->        "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                | Retries _ ->          "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    open Equinox.Cosmos
    type CosmosArguments(a : ParseResults<CosmosParameters>) =
        member __.CfpVerbose =          a.Contains CfpVerbose
        member private __.Suffix =      a.GetResult(LeaseContainerSuffix, "-aux")
        member __.AuxContainerName =    __.Container + __.Suffix
        member __.FromTail =            a.Contains FromTail
        member __.MaxDocuments =        a.TryGetResult MaxDocuments
        member __.LagFrequency =        a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes

        member __.Mode =                a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Connection =          a.TryGetResult CosmosParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =            a.TryGetResult Database |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =           a.TryGetResult Container |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member __.Timeout =             a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries, 1)
        member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds

        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri, _) as discovery) = Discovery.FromConnectionString x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                seconds x.Timeout, x.Retries, seconds x.MaxRetryWaitTime)
            let connector = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, connector
#endif
//#if esdb
    open Equinox.EventStore
    open Propulsion.EventStore
    type [<NoEquality; NoComparison>] EsSourceParameters =
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
        | [<AltCommandLine "-T">]           Tcp
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
                | Tcp ->                    "Request connecting EventStore direct to a TCP/IP endpoint. Default: Use Clustered mode with Gossip-driven discovery (unless environment variable EQUINOX_ES_TCP specifies 'true')."
                | Host _ ->                 "TCP mode: specify EventStore hostname to connect to directly. Clustered mode: use Gossip protocol against all A records returned from DNS query. (optional if environment variable EQUINOX_ES_HOST specified)"
                | Port _ ->                 "specify EventStore custom port. Uses value of environment variable EQUINOX_ES_PORT if specified. Defaults for Cluster and Direct TCP/IP mode are 30778 and 1113 respectively."
                | Username _ ->             "specify username for EventStore. (optional if environment variable EQUINOX_ES_USERNAME specified)"
                | Password _ ->             "specify Password for EventStore. (optional if environment variable EQUINOX_ES_PASSWORD specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds. Default: 1.5."

                | Cosmos _ ->               "CosmosDB (Checkpoint) Store parameters."
    and EsSourceArguments(a : ParseResults<EsSourceParameters>) =
        let discovery (host, port, tcp) =
            match tcp, port with
            | false, None ->   Discovery.GossipDns            host
            | false, Some p -> Discovery.GossipDnsCustomPort (host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", host, p).Uri)
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
        member __.Tcp =                     a.Contains Tcp || isEnvVarTrue "EQUINOX_ES_TCP"
        member __.Port =                    match a.TryGetResult Port with Some x -> Some x | None -> EnvVar.tryGet "EQUINOX_ES_PORT" |> Option.map int
        member __.Host =                    a.TryGetResult Host     |> defaultWithEnvVar "EQUINOX_ES_HOST"     "Host"
        member __.User =                    a.TryGetResult Username |> defaultWithEnvVar "EQUINOX_ES_USERNAME" "Username"
        member __.Password =                a.TryGetResult Password |> defaultWithEnvVar "EQUINOX_ES_PASSWORD" "Password"
        member __.Retries =                 a.GetResult(EsSourceParameters.Retries, 3)
        member __.Timeout =                 a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member __.Heartbeat =               a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds

        member x.Connect(log: ILogger, storeLog: ILogger, appName, nodePreference) =
            let discovery = discovery (x.Host, x.Port, x.Tcp)
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, seconds x.Heartbeat, seconds x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Connect(appName, discovery, nodePreference) |> Async.RunSynchronously

        member __.CheckpointInterval =  TimeSpan.FromHours 1.
        member val Cosmos : CosmosArguments =
            match a.TryGetSubCommand() with
            | Some (EsSourceParameters.Cosmos cosmos) -> CosmosArguments cosmos
            | _ -> raise (MissingArg "Must specify `cosmos` checkpoint store when source is `es`")
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
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
    and CosmosArguments(a : ParseResults<CosmosParameters>) =
        member __.Mode =                    a.GetResult(CosmosParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Connection =              a.TryGetResult CosmosParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.TryGetResult CosmosParameters.Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member __.Timeout =                 a.GetResult(CosmosParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let (Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _) as discovery) = Equinox.Cosmos.Discovery.FromConnectionString x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                seconds x.Timeout, x.Retries, seconds x.MaxRetryWaitTime)
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, x.Database, x.Container, connector
//#endif

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
//#if kafka
        (* Kafka Args *)
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
//#endif
#if cosmos
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<CosmosParameters>
#endif
//#if esdb
        | [<CliPrefix(CliPrefix.None); Last>] Es of ParseResults<EsSourceParameters>
//#endif
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Verbose ->                "Request Verbose Logging. Default: off"
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024"
//#if kafka
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
//#endif
#if cosmos
                | Cosmos _ ->               "specify CosmosDb input parameters"
#endif
//#if esdb
                | Es _ ->                   "specify EventStore input parameters."
//#endif
    and Arguments(a : ParseResults<Parameters>) =
        member __.Verbose =                 a.Contains Verbose
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead, 64)
        member __.MaxConcurrentProcessors = a.GetResult(MaxWriters, 1024)
        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member __.StateInterval =           TimeSpan.FromMinutes 2.
        member x.BuildProcessorParams() =
            Log.Information("Reading {maxPending} ahead; using {dop} processors", x.MaxReadAhead, x.MaxConcurrentProcessors)
            (x.MaxReadAhead, x.MaxConcurrentProcessors)
#if cosmos
        member val Cosmos =                 CosmosArguments(a.GetResult Cosmos)
        member __.BuildChangeFeedParams() =
            let c = __.Cosmos
            match c.MaxDocuments with
            | None -> Log.Information("Processing {leaseId} in {auxContainerName} without document count limit", __.ConsumerGroupName, c.AuxContainerName)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxContainerName} with max {changeFeedMaxDocuments} documents", __.ConsumerGroupName, c.AuxContainerName, lim)
            if c.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            c.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            { database = c.Database; container = c.AuxContainerName }, __.ConsumerGroupName, c.FromTail, c.MaxDocuments, c.LagFrequency
#endif
//#if esdb
        member val Es =                     EsSourceArguments(a.GetResult Es)
        member __.BuildEventStoreParams() =
            let srcE = __.Es
            let startPos, cosmos = srcE.StartPos, srcE.Cosmos
            Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Container {container}",
                __.ConsumerGroupName, startPos, srcE.ForceRestart, cosmos.Database, cosmos.Container)
            Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                srcE.MinBatchSize, srcE.StartingBatchSize, __.MaxReadAhead)
            srcE, cosmos,
                {   groupName = __.ConsumerGroupName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                    forceRestart = srcE.ForceRestart
                    batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = 0 }
//#endif
//#if kafka
        member val Target =                 TargetInfo a
    and TargetInfo(a : ParseResults<Parameters>) =
        member __.Broker =                  a.TryGetResult Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker"
        member __.Topic =                   a.TryGetResult Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
        member x.BuildTargetParams() =      x.Broker, x.Topic
//#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        parser.ParseCommandLine argv |> Arguments

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-logging
// Application logic assumes the global `Serilog.Log` is initialized _immediately_ after a successful ArgumentParser.ParseCommandline
module Logging =

#if cosmos
    let initialize verbose changeFeedProcessorVerbose =
#else
    let initialize verbose =
#endif
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
#if cosmos
            // LibLog writes to the global logger, so we need to control the emission
            |> fun c -> let cfpl = if changeFeedProcessorVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
                        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
                        if changeFeedProcessorVerbose then c else c.Filter.ByExcluding(fun x -> isCfp x)
#endif
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()
//#if esdb

module Checkpoints =

    open Propulsion.EventStore

    // In this implementation, we keep the checkpoints in Cosmos when consuming from EventStore
    module Cosmos =

        let codec = FsCodec.NewtonsoftJson.Codec.Create<Checkpoint.Events.Event>()
        let access = Equinox.Cosmos.AccessStrategy.Custom (Checkpoint.Fold.isOrigin, Checkpoint.Fold.transmute)
        let create groupName (context, cache) =
            let caching = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
            let resolver = Equinox.Cosmos.Resolver(context, codec, Checkpoint.Fold.fold, Checkpoint.Fold.initial, caching, access)
            let resolve streamName = resolver.Resolve(streamName, Equinox.AllowStale)
            Checkpoint.CheckpointSeries(groupName, resolve)

module CosmosContext =

    let create appName (connector : Equinox.Cosmos.Connector) discovery (database, container) =
        let connection = connector.Connect(appName, discovery) |> Async.RunSynchronously
        Equinox.Cosmos.Context(connection, database, container)
//#endif // esdb

let [<Literal>] AppName = "ProjectorTemplate"

let build (args : Args.Arguments) =
    let maxReadAhead, maxConcurrentStreams = args.BuildProcessorParams()
#if cosmos // cosmos
#if     kafka // cosmos && kafka
    let (broker, topic) = args.Target.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic)
#if         parallelOnly // cosmos && kafka && parallelOnly
    let sink = Propulsion.Kafka.ParallelProducerSink.Start(maxReadAhead, maxConcurrentStreams, Handler.render, producer, args.StatsInterval)
#else // cosmos && kafka && !parallelOnly
    let stats = Handler.ProductionStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Kafka.StreamsProducerSink.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.render, producer, stats, args.StatsInterval)
#endif // cosmos && kafka && !parallelOnly
#else // cosmos && !kafka
    let stats = Handler.ProjectorStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.handle, stats, args.StatsInterval)
#endif // cosmos && !kafka
    let createObserver () = CosmosSource.CreateObserver(Log.Logger, sink.StartIngester, Handler.mapToStreamItems)
    let runSourcePipeline =
        let discovery, source, connector = args.Cosmos.BuildConnectionDetails()
        let aux, leaseId, startFromTail, maxDocuments, lagFrequency = args.BuildChangeFeedParams()
        CosmosSource.Run(
            Log.Logger, connector.CreateClient(AppName, discovery), source,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
#endif // cosmos
//#if esdb
    let (srcE, cosmos, spec) = args.BuildEventStoreParams()

    let connectEs () = srcE.Connect(Log.Logger, Log.Logger, AppName, Equinox.EventStore.NodePreference.Master)
    let (discovery, database, container, connector) = cosmos.BuildConnectionDetails()

    let context = CosmosContext.create AppName connector discovery (database, container)
    let cache = Equinox.Cache(AppName, sizeMb=10)

    let checkpoints = Checkpoints.Cosmos.create spec.groupName (context, cache)

#if     kafka // esdb && kafka
    let (broker, topic) = args.Target.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic)
    let stats = Handler.ProductionStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Kafka.StreamsProducerSink.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.render, producer, stats, args.StatsInterval)
#else // esdb && !kafka
    let stats = Handler.ProjectorStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.handle, stats, args.StatsInterval)
#endif // esdb && !kafka
    let runSourcePipeline =
        let filterByStreamName _ = true // see `dotnet new proReactor --filter` for an example of how to rig filtering arguments
        Propulsion.EventStore.EventStoreSource.Run(
            Log.Logger, sink, checkpoints, connectEs, spec, Handler.tryMapEvent filterByStreamName,
            args.MaxReadAhead, args.StatsInterval)
//#endif // esdb
    sink, runSourcePipeline

let run args =
    let sink, runSourcePipeline = build args
    runSourcePipeline |> Async.Start
    sink.AwaitCompletion() |> Async.RunSynchronously
    sink.RanToCompletion

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
#if cosmos
        try Logging.initialize args.Verbose args.Cosmos.CfpVerbose
#else
        try Logging.initialize args.Verbose
#endif
            try Configuration.initialize ()
                if run args then 0 else 3
            with e when not (e :? Args.MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
