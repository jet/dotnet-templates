module ProjectorTemplate.Program

#if cosmos
open Propulsion.Cosmos
#endif
open Serilog
open System

exception MissingArg of message : string with override this.Message = this.message

type Configuration(tryGet) =

    let get key =
        match tryGet key with
        | Some value -> value
        | None -> raise (MissingArg (sprintf "Missing Argument/Environment Variable %s" key))
#if esdb
    let isTrue varName = tryGet varName |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))
#endif
#if cosmos
    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"
#endif
//#if sss
    member _.SqlStreamStoreConnection =     get "SQLSTREAMSTORE_CONNECTION"
    member _.SqlStreamStoreCredentials =    tryGet "SQLSTREAMSTORE_CREDENTIALS"
    member _.SqlStreamStoreCredentialsCheckpoints = tryGet "SQLSTREAMSTORE_CREDENTIALS_CHECKPOINTS"
    member _.SqlStreamStoreDatabase =       get "SQLSTREAMSTORE_DATABASE"
    member _.SqlStreamStoreContainer =      get "SQLSTREAMSTORE_CONTAINER"
//#endif
#if esdb
    member _.EventStoreHost =               get "EQUINOX_ES_HOST"
    member _.EventStoreTcp =                isTrue "EQUINOX_ES_TCP"
    member _.EventStorePort =               tryGet "EQUINOX_ES_PORT" |> Option.map int
    member _.EventStoreUsername =           get "EQUINOX_ES_USERNAME"
    member _.EventStorePassword =           get "EQUINOX_ES_PASSWORD"
#endif
//#if kafka
    member _.Broker =                       get "PROPULSION_KAFKA_BROKER"
    member _.Topic =                        get "PROPULSION_KAFKA_TOPIC"
//#endif

module Args =

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
    type CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        member val CfpVerbose =         a.Contains CfpVerbose
        member val private Suffix =  a.GetResult(LeaseContainerSuffix, "-aux")
        member val AuxContainerName =   Container + Suffix
        member val FromTail =           a.Contains FromTail
        member val MaxDocuments =       a.TryGetResult MaxDocuments
        member val LagFrequency =       a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes

        member val Mode =               a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member val Connection =         a.TryGetResult CosmosParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Discovery.FromConnectionString
        member val Database =           a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =          a.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        member val Timeout =            a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =            a.GetResult(Retries, 1)
        member val MaxRetryWaitTime =   a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds

        member x.MonitoringParams() =
            let Discovery.UriAndKey (endpointUri, _) as discovery = x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                TimeSpan.FromSeconds x.Timeout, x.Retries, TimeSpan.FromSeconds x.MaxRetryWaitTime)
            let connector = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            connector, discovery, { database = x.Database; container = x.Container }
#endif
#if esdb
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
    and EsSourceArguments(c : Configuration, a : ParseResults<EsSourceParameters>) =
        let discovery (host, port, tcp) =
            match tcp, port with
            | false, None ->   Discovery.GossipDns            host
            | false, Some p -> Discovery.GossipDnsCustomPort (host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", host, p).Uri)
        member val Gorge =                  a.TryGetResult Gorge
        member val TailInterval =           a.GetResult(Tail, 1.) |> TimeSpan.FromSeconds
        member val ForceRestart =           a.Contains ForceRestart
        member val StartingBatchSize =      a.GetResult(BatchSize, 4096)
        member val MinBatchSize =           a.GetResult(MinBatchSize, 512)
        member val StartPos =
            match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent, a.Contains EsSourceParameters.FromTail with
            | Some p, _, _, _ ->            Absolute p
            | _, Some c, _, _ ->            StartPos.Chunk c
            | _, _, Some p, _ ->            Percentage p
            | None, None, None, true ->     StartPos.TailOrCheckpoint
            | None, None, None, _ ->        StartPos.StartOrCheckpoint
        member val Tcp =                    a.Contains Tcp || c.EventStoreTcp
        member val Port =                   match a.TryGetResult Port with Some x -> Some x | None -> c.EventStorePort
        member val Host =                   a.TryGetResult Host     |> Option.defaultWith (fun () -> c.EventStoreHost)
        member val User =                   a.TryGetResult Username |> Option.defaultWith (fun () -> c.EventStoreUsername)
        member val Password =               a.TryGetResult Password |> Option.defaultWith (fun () -> c.EventStorePassword)
        member val Retries =                a.GetResult(EsSourceParameters.Retries, 3)
        member val Timeout =                a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member val Heartbeat =              a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds

        member x.Connect(log: ILogger, storeLog: ILogger, appName, nodePreference) =
            let discovery = discovery (x.Host, x.Port, x.Tcp)
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, TimeSpan.FromSeconds x.Heartbeat, TimeSpan.FromSeconds x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Connect(appName, discovery, nodePreference) |> Async.RunSynchronously

        member val CheckpointInterval =     TimeSpan.FromHours 1.
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
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        member val Mode =                   a.GetResult(CosmosParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member val Connection =             a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Discovery.FromConnectionString
        member val Database =               a.TryGetResult CosmosSourceParameters.Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.TryGetResult CosmosSourceParameters.Container  |> Option.defaultWith (fun () -> c.CosmosContainer)
        member val Timeout =                a.GetResult(CosmosParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =                a.GetResult(CosmosParameters.Retries, 1)
        member val MaxRetryWaitTime =       a.GetResult(CosmosParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _) as discovery = x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                TimeSpan.FromSeconds x.Timeout, x.Retries, TimeSpan.FromSeconds x.MaxRetryWaitTime)
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, x.Database, x.Container, connector
#endif
//#if sss
    // TOCONSIDER: add DB connectors other than MsSql
    type [<NoEquality; NoComparison>] SqlStreamStoreSourceParameters =
        | [<AltCommandLine "-t"; Unique>]   Tail of intervalS: float
        | [<AltCommandLine "-m"; Unique>]   BatchSize of int
        | [<AltCommandLine "-c"; Unique>]   Connection of string
        | [<AltCommandLine "-p"; Unique>]   Credentials of string
        | [<AltCommandLine "-s">]           Schema of string
        | [<AltCommandLine "-cc"; Unique>]  CheckpointsConnection of string
        | [<AltCommandLine "-cp"; Unique>] CheckpointsCredentials of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Tail _ ->                 "Polling interval in Seconds. Default: 1"
                | BatchSize _ ->            "Maximum events to request from feed. Default: 512"
                | Connection _ ->           "Connection string for SqlStreamStore db. Optional if SQLSTREAMSTORE_CONNECTION specified"
                | Credentials _ ->          "Credentials string for SqlStreamStore db (used as part of connection string, but NOT logged). Default: use SQLSTREAMSTORE_CREDENTIALS environment variable (or assume no credentials)"
                | Schema _ ->               "Database schema name"
                | CheckpointsConnection _ ->"Connection string for Checkpoints sql db. Optional if SQLSTREAMSTORE_CONNECTION_CHECKPOINTS specified. Default: same as `Connection`"
                | CheckpointsCredentials _ ->"Credentials string for Checkpoints sql db. (used as part of checkpoints connection string, but NOT logged). Default (when no `CheckpointsConnection`: use `Credentials. Default (when `CheckpointsConnection` specified): use SQLSTREAMSTORE_CREDENTIALS_CHECKPOINTS environment variable (or assume no credentials)"
    and SqlStreamStoreSourceArguments(c : Configuration, a : ParseResults<SqlStreamStoreSourceParameters>) =
        member val TailInterval =           a.GetResult(Tail, 1.) |> TimeSpan.FromSeconds
        member val MaxBatchSize =           a.GetResult(BatchSize, 512)
        member val private Connection =     a.TryGetResult Connection |> Option.defaultWith (fun () -> c.SqlStreamStoreConnection)
        member val private Credentials =    a.TryGetResult Credentials |> Option.orElseWith (fun () -> c.SqlStreamStoreCredentials) |> Option.toObj
        member val Schema =                 a.GetResult(Schema, null)

        member x.BuildCheckpointsConnectionString() =
            let c, cs =
                match a.TryGetResult CheckpointsConnection, a.TryGetResult CheckpointsCredentials with
                | Some c, Some p -> c, String.Join(";", c, p)
                | None, Some p ->   let c = x.Connection in c, String.Join(";", c, p)
                | None, None ->     let c = x.Connection in c, String.Join(";", c, x.Credentials)
                | Some cc, None ->  let p = c.SqlStreamStoreCredentialsCheckpoints |> Option.toObj
                                    cc, String.Join(";", cc, p)
            Log.Information("Checkpoints MsSql Connection {connectionString}", c)
            cs
        member x.Connect() =
            let conn, creds, schema, autoCreate = x.Connection, x.Credentials, x.Schema, false
            let sssConnectionString = String.Join(";", conn, creds)
            Log.Information("SqlStreamStore MsSql Connection {connectionString} Schema {schema} AutoCreate {autoCreate}", conn, schema, autoCreate)
            Equinox.SqlStreamStore.MsSql.Connector(sssConnectionString, schema, autoCreate=autoCreate).Connect() |> Async.RunSynchronously
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
#if esdb
        | [<CliPrefix(CliPrefix.None); Last>] Es of ParseResults<EsSourceParameters>
#endif
//#if sss
        | [<CliPrefix(CliPrefix.None); AltCommandLine "ms"; Last>] SqlMs of ParseResults<SqlStreamStoreSourceParameters>
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
#if esdb
                | Es _ ->                   "specify EventStore input parameters."
#endif
//#if sss
                | SqlMs _ ->                "specify SqlStreamStore input parameters."
//#endif
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val Verbose =                a.Contains Verbose
        member val ConsumerGroupName =      a.GetResult ConsumerGroupName
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 64)
        member val MaxConcurrentProcessors =a.GetResult(MaxWriters, 1024)
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 2.
        member x.BuildProcessorParams() =
            Log.Information("Projecting... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxConcurrentProcessors, x.MaxReadAhead)
            (x.MaxReadAhead, x.MaxConcurrentProcessors)
#if cosmos
        member val Cosmos =                 CosmosArguments(a.GetResult Cosmos)
        member x.BuildChangeFeedParams() =
            let c = x.Cosmos
            match c.MaxDocuments with
            | None -> Log.Information("Processing {leaseId} in {auxContainerName} without document count limit", x.ConsumerGroupName, c.AuxContainerName)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxContainerName} with max {changeFeedMaxDocuments} documents", x.ConsumerGroupName, c.AuxContainerName, lim)
            if c.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            c.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            { database = c.Database; container = c.AuxContainerName }, x.ConsumerGroupName, c.FromTail, c.MaxDocuments, c.LagFrequency
#endif
#if esdb
        member val Es =                     EsSourceArguments(a.GetResult Es)
        member x.BuildEventStoreParams() =
            let srcE = x.Es
            let startPos, cosmos = srcE.StartPos, srcE.Cosmos
            Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Container {container}",
                x.ConsumerGroupName, startPos, srcE.ForceRestart, cosmos.Database, cosmos.Container)
            Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                srcE.MinBatchSize, srcE.StartingBatchSize, x.MaxReadAhead)
            srcE, cosmos,
                {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                    forceRestart = srcE.ForceRestart
                    batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = 0 }
#endif
//#if sss
        member val SqlStreamStore =         SqlStreamStoreSourceArguments(c, a.GetResult SqlMs)
        member x.BuildSqlStreamStoreParams() =
            let src = x.SqlStreamStore
            let spec : Propulsion.SqlStreamStore.ReaderSpec =
                {    consumerGroup          = x.ConsumerGroupName
                     maxBatchSize           = src.MaxBatchSize
                     tailSleepInterval      = src.TailInterval }
            src, spec
//#endif
//#if kafka
        member val Target =                 TargetInfo (c, a)
    and TargetInfo(c : Configuration, a : ParseResults<Parameters>) =
        member val Broker =                 a.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  a.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member x.BuildTargetParams() =      x.Broker, x.Topic
//#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

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
    let broker, topic = args.Target.BuildTargetParams()
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
    let pipeline =
        let monitoredConnector, monitoredDiscovery, monitored = args.Cosmos.MonitoringParams()
        let aux, leaseId, startFromTail, maxDocuments, lagFrequency = args.BuildChangeFeedParams()
        let createObserver () = CosmosSource.CreateObserver(Log.ForContext<CosmosSource>(), sink.StartIngester, Handler.mapToStreamItems)
        CosmosSource.Run(
            Log.Logger, monitoredConnector.CreateClient(AppName, monitoredDiscovery), monitored,
            aux, leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
#endif // cosmos
#if esdb
    let srcE, cosmos, spec = args.BuildEventStoreParams()

    let connectEs () = srcE.Connect(Log.Logger, Log.Logger, AppName, Equinox.EventStore.NodePreference.Master)
    let discovery, database, container, connector = cosmos.BuildConnectionDetails()

    let context = CosmosContext.create AppName connector discovery (database, container)
    let cache = Equinox.Cache(AppName, sizeMb=10)

    let checkpoints = Checkpoints.Cosmos.create spec.groupName (context, cache)

#if     kafka // esdb && kafka
    let broker, topic = args.Target.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic)
    let stats = Handler.ProductionStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Kafka.StreamsProducerSink.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.render, producer, stats, args.StatsInterval)
#else // esdb && !kafka
    let stats = Handler.ProjectorStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.handle, stats, args.StatsInterval)
#endif // esdb && !kafka
    let pipeline =
        let filterByStreamName _ = true // see `dotnet new proReactor --filter` for an example of how to rig filtering arguments
        Propulsion.EventStore.EventStoreSource.Run(
            Log.Logger, sink, checkpoints, connectEs, spec, Handler.tryMapEvent filterByStreamName,
            args.MaxReadAhead, args.StatsInterval)
#endif // esdb
//#if sss
    let srcSql, spec = args.BuildSqlStreamStoreParams()

    let monitored = srcSql.Connect()

    let connectionString = srcSql.BuildCheckpointsConnectionString()
    let checkpointer = Propulsion.SqlStreamStore.SqlCheckpointer(connectionString)

#if     kafka // sss && kafka
    let broker, topic = args.Target.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic)
    let stats = Handler.ProductionStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Kafka.StreamsProducerSink.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.render, producer, stats, args.StatsInterval)
#else // sss && !kafka
    let stats = Handler.ProjectorStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, Handler.handle, stats, args.StatsInterval)
#endif // sss && !kafka
    let pipeline = Propulsion.SqlStreamStore.SqlStreamStoreSource.Run(Log.Logger, monitored, checkpointer, spec, sink, args.StatsInterval)
//#endif // sss
    sink, pipeline

let run args = async {
    let sink, pipeline = build args
    pipeline |> Async.Start
    do! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
#if cosmos
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose, changeFeedProcessorVerbose=args.Cosmos.CfpVerbose).CreateLogger()
#else
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
#endif
            try run args  |> Async.RunSynchronously
                0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
