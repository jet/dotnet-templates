module ReactorTemplate.Program

//#if (!kafkaEventSpans)
//#if multiSource
open Propulsion.EventStore
//#endif
//#endif
open Serilog
open System

exception MissingArg of message : string with override this.Message = this.message

type Configuration(tryGet) =

    let get key =
        match tryGet key with
        | Some value -> value
        | None -> raise (MissingArg (sprintf "Missing Argument/Environment Variable %s" key))
    let isTrue varName = tryGet varName |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"
    member _.EventStoreTcp =                isTrue "EQUINOX_ES_TCP"
    member _.EventStoreProjectionTcp =      isTrue "EQUINOX_ES_PROJ_TCP"
    member _.EventStorePort =               tryGet "EQUINOX_ES_PORT" |> Option.map int
    member _.EventStoreProjectionPort =     tryGet "EQUINOX_ES_PROJ_PORT" |> Option.map int
    member _.EventStoreHost =               get "EQUINOX_ES_HOST"
    member _.EventStoreProjectionHost =     tryGet "EQUINOX_ES_PROJ_HOST"
    member _.EventStoreUsername =           get "EQUINOX_ES_USERNAME"
    member _.EventStoreProjectionUsername = tryGet "EQUINOX_ES_PROJ_USERNAME"
    member _.EventStorePassword =           get "EQUINOX_ES_PASSWORD"
    member _.EventStoreProjectionPassword = tryGet "EQUINOX_ES_PROJ_PASSWORD"
    member _.Broker =                       get "PROPULSION_KAFKA_BROKER"
    member _.Topic =                        get "PROPULSION_KAFKA_TOPIC"

module Args =

    open Argu
//#if multiSource
    open Equinox.EventStore
//#endif
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-V"; Unique>]   Verbose
//#if (!kafkaEventSpans)
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose
//#endif
//#if filter

        | [<AltCommandLine "-e">]           CategoryBlacklist of string
        | [<AltCommandLine "-i">]           CategoryWhitelist of string
//#endif
#if kafkaEventSpans
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSourceParameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSourceParameters>
//#if multiSource
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Es of ParseResults<EsSourceParameters>
//#endif
#endif
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | Verbose ->                "request Verbose Logging. Default: off."
//#if (!kafkaEventSpans)
                | CfpVerbose ->             "request Verbose Change Feed Processor Logging. Default: off."
//#endif
//#if filter
                | CategoryBlacklist _ ->    "category whitelist"
                | CategoryWhitelist _ ->    "category blacklist"
//#endif
#if (!kafkaEventSpans)
                | Cosmos _ ->               "specify CosmosDB input parameters."
//#if multiSource
                | Es _ ->                   "specify EventStore input parameters."
//#endif
#else
                | Kafka _ ->                "specify Kafka input parameters."
#endif
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val ConsumerGroupName =      a.GetResult ConsumerGroupName
//#if (!kafkaEventSpans)
        member val CfpVerbose =             a.Contains CfpVerbose
//#endif
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 16)
        member val MaxConcurrentStreams =   a.GetResult(MaxWriters, 8)
        member val Verbose =                a.Contains Parameters.Verbose
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
//#if filter
        member _.FilterFunction(?excludeLong, ?longOnly): string -> bool =
            let isLong (streamName : string) =
                streamName.StartsWith "Inventory-" // Too long
                || streamName.StartsWith "InventoryCount-" // No Longer used
                || streamName.StartsWith "InventoryLog" // 5GB, causes lopsided partitions, unused
            let excludeLong = defaultArg excludeLong true
            match a.GetResults CategoryBlacklist, a.GetResults CategoryWhitelist with
            | [], [] when longOnly = Some true ->
                Log.Information("Only including long streams")
                isLong
            | [], [] ->
                let black = set [
                    "SkuFileUpload-534e4362c641461ca27e3d23547f0852"
                    "SkuFileUpload-778f1efeab214f5bab2860d1f802ef24"
                    "PurchaseOrder-5791" ]
                let isCheckpoint (streamName : string) =
                    streamName.EndsWith "_checkpoint"
                    || streamName.EndsWith "_checkpoints"
                    || streamName.StartsWith "#serial"
                    || streamName.StartsWith "marvel_bookmark"
                Log.Information("Using well-known stream blacklist {black} excluding checkpoints and #serial streams, excluding long streams: {excludeLong}", black, excludeLong)
                fun x -> not (black.Contains x) && (not << isCheckpoint) x && (not excludeLong || (not << isLong) x)
            | bad, [] ->    let black = Set.ofList bad in Log.Warning("Excluding categories: {cats}", black); fun x -> not (black.Contains x)
            | [], good ->   let white = Set.ofList good in Log.Warning("Only copying categories: {cats}", white); fun x -> white.Contains x
            | _, _ -> raise (MissingArg "BlackList and Whitelist are mutually exclusive; inclusions and exclusions cannot be mixed")
//#endif
#if changeFeedOnly
        member val Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Cosmos cosmos) -> CosmosSourceArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Src")
        member x.SourceParams() =
                let srcC = x.Source
#endif
//#if (!kafkaEventSpans)
//#if (!changeFeedOnly)
        member val Source : Choice<EsSourceArguments, CosmosSourceArguments> =
            match a.TryGetSubCommand() with
            | Some (Es es) -> Choice1Of2 (EsSourceArguments (c, es))
            | Some (Parameters.Cosmos cosmos) -> Choice2Of2 (CosmosSourceArguments (c, cosmos))
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")
        member x.SourceParams() : Choice<EsSourceArguments*Equinox.CosmosStore.CosmosStoreContext*ReaderSpec, _> =
            match x.Source with
            | Choice1Of2 srcE ->
                let startPos, cosmos = srcE.StartPos, srcE.Cosmos
                Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Container {container}",
                    x.ConsumerGroupName, startPos, srcE.ForceRestart, cosmos.DatabaseId, cosmos.ContainerId)
                Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                    srcE.MinBatchSize, srcE.StartingBatchSize, x.MaxReadAhead)
                let context = cosmos.Connect() |> Async.RunSynchronously |> CosmosStoreContext.create
                Choice1Of2 (srcE, context,
                    {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                        forceRestart = srcE.ForceRestart
                        batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = 0 })
            | Choice2Of2 srcC ->
//#endif // !changeFeedOnly
                let leases = srcC.ConnectLeases()
                Log.Information("Reacting... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxConcurrentStreams, x.MaxReadAhead)
                Log.Information("Monitoring Group {processorName} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                    x.ConsumerGroupName, srcC.DatabaseId, srcC.ContainerId, Option.toNullable srcC.MaxDocuments)
                if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
                srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
                let storeClient, monitored = srcC.ConnectStoreAndMonitored()
                let context = CosmosStoreContext.create storeClient
#if changeFeedOnly
                (context, monitored, leases, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency)
#else
                Choice2Of2 (context, monitored, leases, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency)
#endif
//#endif // kafkaEventSpans
#if kafkaEventSpans
        member val Source : KafkaSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Kafka kafka) -> KafkaSourceArguments (c, kafka)
            | _ -> raise (MissingArg "Must specify kafka for Src")
     and [<NoEquality; NoComparison>] KafkaSourceParameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        | [<AltCommandLine "-m"; Unique>]   MaxInflightMb of float
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
#if (kafka && blank)
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
#endif
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
                | MaxInflightMb _ ->        "maximum MiB of data to read ahead. Default: 10."
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off."
#if (kafka && blank)
                | Kafka _ ->                "Kafka Source parameters."
#else
                | Cosmos _ ->               "CosmosDb Sink parameters."
#endif
    and KafkaSourceArguments(c : Configuration, a : ParseResults<KafkaSourceParameters>) =
        member val Broker =                 a.TryGetResult KafkaSourceParameters.Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  a.TryGetResult KafkaSourceParameters.Topic  |> Option.defaultWith (fun () -> c.Topic)
        member val MaxInFlightBytes =       a.GetResult(MaxInflightMb, 10.) * 1024. * 1024. |> int64
        member val LagFrequency =           a.TryGetResult LagFreqM |> Option.map System.TimeSpan.FromMinutes
        member x.BuildSourceParams() =      x.Broker, x.Topic
#if (kafka && blank)
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (KafkaSourceParameters.Kafka kafka) -> KafkaSinkArguments (c, kafka)
            | _ -> raise (MissingArg "Must specify kafka arguments")
#else
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (KafkaSourceParameters.Cosmos cosmos) -> CosmosArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos details")
#endif
#else
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-md"; Unique>]  MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Unique>]   Container of string // Actually Mandatory, but stating that is not supported
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float

#if (!multiSource && kafka && blank)
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
#endif
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: off"
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name within `Database`"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

#if (!multiSource && kafka && blank)
                | Kafka _ ->                "Kafka Source parameters."
#else
                | Cosmos _ ->               "CosmosDb Sink parameters."
#endif
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        let discovery =                     a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult CosmosSourceParameters.ConnectionMode
        let timeout =                       a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSourceParameters.Retries, 1)
        let maxRetryWaitTime =              a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        member val DatabaseId =             a.TryGetResult CosmosSourceParameters.Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val ContainerId =            a.GetResult CosmosSourceParameters.Container
        //member x.MonitoredContainer() =     connector.ConnectMonitored(x.DatabaseId, x.ContainerId)
        
        member val FromTail =               a.Contains CosmosSourceParameters.FromTail
        member val MaxDocuments =           a.TryGetResult MaxDocuments
        member val LagFrequency =           a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member val private LeaseContainerId = a.TryGetResult CosmosSourceParameters.LeaseContainer
        member private x.ConnectLeases containerId = connector.CreateUninitialized(x.DatabaseId, containerId)
        member x.ConnectLeases() =          match x.LeaseContainerId with
                                            | None ->    x.ConnectLeases(x.ContainerId + "-aux")
                                            | Some sc -> x.ConnectLeases(sc)
        member x.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(x.DatabaseId, x.ContainerId)
#if (!multiSource && kafka && blank)
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Kafka kafka) -> KafkaSinkArguments (c, kafka)
            | _ -> raise (MissingArg "Must specify `kafka` arguments")
#else
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Cosmos cosmos) -> CosmosArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos details")
#endif
//#if multiSource
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
        | [<AltCommandLine "-T">]           Tcp
        | [<AltCommandLine "-h">]           Host of string
        | [<AltCommandLine "-x">]           Port of int
        | [<AltCommandLine "-u">]           Username of string
        | [<AltCommandLine "-p">]           Password of string
        | [<AltCommandLine "-Tp">]          ProjTcp
        | [<AltCommandLine "-hp">]          ProjHost of string
        | [<AltCommandLine "-xp">]          ProjPort of int
        | [<AltCommandLine "-up">]          ProjUsername of string
        | [<AltCommandLine "-pp">]          ProjPassword of string

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
                | ProjTcp ->                "Request connecting Projection EventStore direct to a TCP/IP endpoint. Default: Use Clustered mode with Gossip-driven discovery (unless environment variable EQUINOX_ES_PROJ_TCP specifies 'true')."
                | ProjHost _ ->             "TCP mode: specify Projection EventStore hostname to connect to directly. Clustered mode: use Gossip protocol against all A records returned from DNS query. Defaults to value of es host (-h) unless environment variable EQUINOX_ES_PROJ_HOST is specified."
                | ProjPort _ ->             "specify Projection EventStore custom port. Defaults to value of es port (-x) unless environment variable EQUINOX_ES_PROJ_PORT is specified."
                | ProjUsername _ ->         "specify username for Projection EventStore. Defaults to value of es user (-u) unless environment variable EQUINOX_ES_PROJ_USERNAME is specified."
                | ProjPassword _ ->         "specify Password for Projection EventStore. Defaults to value of es password (-p) unless environment variable EQUINOX_ES_PROJ_PASSWORD is specified."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds. Default: 1.5."

                | Cosmos _ ->               "CosmosDB (Checkpoint) Store parameters."
    and EsSourceArguments(c : Configuration, a : ParseResults<EsSourceParameters>) =
        let ts (x : TimeSpan) = x.TotalSeconds
        let discovery (host, port, tcp) =
            match tcp, port with
            | false, None ->   Discovery.GossipDns            host
            | false, Some p -> Discovery.GossipDnsCustomPort (host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", host, p).Uri)
        let tcp =                           a.Contains Tcp || c.EventStoreTcp
        let host =                          a.TryGetResult Host |> Option.defaultWith (fun () -> c.EventStoreHost)
        let port =                          a.TryGetResult Port |> Option.orElseWith (fun () -> c.EventStorePort)
        let user =                          a.TryGetResult Username |> Option.defaultWith (fun () -> c.EventStoreUsername)
        let password =                      a.TryGetResult Password |> Option.defaultWith (fun () -> c.EventStorePassword)
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
        member val Tcp =                    tcp
        member val Host =                   host
        member val Port =                   port
        member val User =                   user
        member val Password =               password
        member val ProjTcp =                a.Contains ProjTcp || c.EventStoreProjectionTcp
        member val ProjPort =               match a.TryGetResult ProjPort with
                                            | Some x -> Some x
                                            | None -> c.EventStoreProjectionPort |> Option.orElse port
        member val ProjHost =               match a.TryGetResult ProjHost with
                                            | Some x -> x
                                            | None -> c.EventStoreProjectionHost |> Option.defaultValue host
        member val ProjUser =               match a.TryGetResult ProjUsername with
                                            | Some x -> x
                                            | None -> c.EventStoreProjectionUsername |> Option.defaultValue user
        member val ProjPassword =           match a.TryGetResult ProjPassword with
                                            | Some x -> x
                                            | None -> c.EventStoreProjectionPassword |> Option.defaultValue password
        member val Retries =                a.GetResult(EsSourceParameters.Retries, 3)
        member val Timeout =                a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member val Heartbeat =              a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
        
        member x.ConnectProj(log: ILogger, storeLog: ILogger, appName, nodePreference) =
            let discovery = discovery (x.ProjHost, x.ProjPort, x.ProjTcp)
            log.ForContext("projHost", x.ProjHost).ForContext("projPort", x.ProjPort)
                .Information("Projection EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, ts x.Heartbeat, ts x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.ProjUser, x.ProjPassword, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Connect(appName + "-Proj", discovery, nodePreference) |> Async.RunSynchronously
        
        member x.Connect(log: ILogger, storeLog: ILogger, appName, connectionStrategy) =
            let discovery = discovery (x.Host, x.Port, x.Tcp)
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, ts x.Heartbeat, ts x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Establish(appName, discovery, connectionStrategy) |> Async.RunSynchronously

        member val CheckpointInterval =     TimeSpan.FromHours 1.
        member val Cosmos : CosmosArguments =
            match a.TryGetSubCommand() with
            | Some (EsSourceParameters.Cosmos cosmos) -> CosmosArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify `cosmos` checkpoint store when source is `es`")
//#endif
#endif
//#if (multiSource || !(blank && kafka))
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
//#if kafka
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
//#endif
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
//#if kafka
                | Kafka _ ->                "Kafka Sink parameters."
//#endif
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        let discovery =                     a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult ConnectionMode
        let timeout =                       a.GetResult(Timeout, 30.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(Retries, 9)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode=mode)
        member val DatabaseId =             a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val ContainerId =            a.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        member x.Connect() =                connector.ConnectStore("Main", x.DatabaseId, x.ContainerId)
        member x.ConnectLeases() =          connector.CreateUninitialized(x.DatabaseId, x.ContainerId)
        member x.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(x.DatabaseId, x.ContainerId)
//#if kafka
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (CosmosParameters.Kafka kafka) -> KafkaSinkArguments (c, kafka)
            | _ -> raise (MissingArg "Must specify `kafka` arguments")
//#endif
//#endif // (!(!multiSource && kafka && blank))
//#if kafka
     and [<NoEquality; NoComparison>] KafkaSinkParameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
    and KafkaSinkArguments(c : Configuration, a : ParseResults<KafkaSinkParameters>) =
        member val Broker =                 a.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  a.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member x.BuildTargetParams() =      x.Broker, x.Topic
//#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ReactorTemplate"

//#if multiSource
#if (!kafkaEventSpans)
//#if (!changeFeedOnly)
module Checkpoints =

    open Equinox.CosmosStore

    // In this implementation, we keep the checkpoints in Cosmos when consuming from EventStore
    module Cosmos =

        let codec = FsCodec.NewtonsoftJson.Codec.Create<Checkpoint.Events.Event>()
        let access = AccessStrategy.Custom (Checkpoint.Fold.isOrigin, Checkpoint.Fold.transmute)
        let create groupName (context, cache) =
            let caching = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
            let cat = CosmosStoreCategory(context, codec, Checkpoint.Fold.fold, Checkpoint.Fold.initial, caching, access)
            let resolve streamName = cat.Resolve(streamName, Equinox.AllowStale)
            Checkpoint.CheckpointSeries(groupName, resolve)

//#endif
#endif
module EventStoreContext =

    let create connection =
        Equinox.EventStore.Context(connection, Equinox.EventStore.BatchingPolicy(maxBatchSize=500))

//#endif
let build (args : Args.Arguments) =
#if (!kafkaEventSpans)
//#if (!changeFeedOnly)
    match args.SourceParams() with
    | Choice1Of2 (srcE, context, spec) ->
        let connectEs () = srcE.Connect(Log.Logger, Log.Logger, AppName, Equinox.EventStore.ConnectionStrategy.ClusterSingle Equinox.EventStore.NodePreference.Master)
        let connectProjEs () = srcE.ConnectProj(Log.Logger, Log.Logger, AppName, Equinox.EventStore.NodePreference.PreferSlave)

        let cache = Equinox.Cache(AppName, sizeMb=10)

        let checkpoints = Checkpoints.Cosmos.create spec.groupName (context, cache)
#if kafka
        let broker, topic = srcE.Cosmos.Sink.BuildTargetParams()
        let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, Confluent.Kafka.Acks.All, topic)
        let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
            producer.Produce(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)
#if blank
        let handle = Handler.handle produceSummary
#else
        let esConn = connectEs ()
        let srcCache = Equinox.Cache(AppName, sizeMb=10)
        let srcService = Todo.EventStore.create (EventStoreContext.create esConn, srcCache)
        let handle = Handler.handle srcService produceSummary
#endif
        let stats = Handler.Stats(Log.Logger, args.StatsInterval, args.StateInterval, logExternalStats=producer.DumpStats)
        let sink =
#if (kafka && !blank)
             Propulsion.Streams.Sync.StreamsSync.Start(
                 Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle,
                 stats, statsInterval=args.StatsInterval)
#else
             Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats, args.StatsInterval)
#endif
#else // !kafka -> ingestion
#if blank
        // TODO: establish any relevant inputs, or re-run without `--blank` for example wiring code
        let handle = Ingester.handle
#else // blank
        let esConn = connectEs ()
        let srcCache = Equinox.Cache(AppName, sizeMb=10)
        let srcService = Todo.EventStore.create (EventStoreContext.create esConn, srcCache)
        let dstService = TodoSummary.Cosmos.create (context, cache)
        let handle = Ingester.handle srcService dstService
#endif // blank
        let stats = Ingester.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
        let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats, args.StatsInterval)
#endif // !kafka
#if filter
        let filterByStreamName = args.FilterFunction()
#else
        let filterByStreamName _ = true
#endif
        let runPipeline =
            EventStoreSource.Run(
                Log.Logger, sink, checkpoints, connectProjEs, spec, Handler.tryMapEvent filterByStreamName,
                args.MaxReadAhead, args.StatsInterval)
        sink, runPipeline
    | Choice2Of2 (context, monitored, leases, processorName, startFromTail, maxDocuments, lagFrequency) ->
//#endif // changeFeedOnly
#if changeFeedOnly
        let context, monitored, leases, processorName, startFromTail, maxDocuments, lagFrequency = args.SourceParams()
#endif
#else // !kafkaEventSpans -> wire up consumption from Kafka, with auxiliary `cosmos` store
        let consumerConfig =
            FsKafka.KafkaConsumerConfig.Create(
                AppName, source.Broker, [source.Topic], args.ConsumerGroupName, Confluent.Kafka.AutoOffsetReset.Earliest,
                maxInFlightBytes = source.MaxInFlightBytes, ?statisticsInterval = source.LagFrequency)
#endif // kafkaEventSpans

#if kafka
#if (blank && !multiSource)
        let broker, topic = source.Sink.BuildTargetParams()
#else
        let broker, topic = source.Cosmos.Sink.BuildTargetParams()
#endif
        let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, Confluent.Kafka.Acks.All, topic)
        let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
            producer.Produce(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)
#if blank
        let handle = Handler.handle produceSummary
#else
        let cache = Equinox.Cache(AppName, sizeMb=10)
        let service = Todo.Cosmos.create (context, cache)
        let handle = Handler.handle service produceSummary
#endif
        let stats = Handler.Stats(Log.Logger, args.StatsInterval, args.StateInterval, logExternalStats=producer.DumpStats)
#else // !kafka -> Ingester only
#if blank
        // TODO: establish any relevant inputs, or re-run without `-blank` for example wiring code
        let handle = Ingester.handle
#else // blank -> no specific Ingester source/destination wire-up
        let cache = Equinox.Cache(AppName, sizeMb=10)
        let srcService = Todo.Cosmos.create (context, cache)
        let dstService = TodoSummary.Cosmos.create (context, cache)
        let handle = Ingester.handle srcService dstService
#endif // blank
        let stats = Ingester.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
#endif // kafka
#if filter
        let filterByStreamName = args.FilterFunction()
#endif
#if kafkaEventSpans

        let parseStreamEvents (res : Confluent.Kafka.ConsumeResult<_, _>) : seq<Propulsion.Streams.StreamEvent<_>> =
            Propulsion.Codec.NewtonsoftJson.RenderedSpan.parse res.Message.Value
#if filter
            |> Seq.filter (fun e -> e.stream |> FsCodec.StreamName.toString |> filterByStreamName)
#endif
        Propulsion.Kafka.StreamsConsumer.Start
            (   Log.Logger, consumerConfig, parseStreamEvents, handle, args.MaxConcurrentStreams,
                stats=stats, statsInterval=args.StateInterval)
#else // !kafkaEventSpans => Default consumption, from CosmosDb
#if (kafka && !blank)
        let sink =
            Propulsion.Streams.Sync.StreamsSync.Start(
                 Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle,
                 stats, statsInterval=args.StateInterval)
#else
        let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats, args.StatsInterval)
#endif

        let mapToStreamItems docs : Propulsion.Streams.StreamEvent<_> seq =
            // TODO: customize parsing to events if source is not an Equinox Container
            docs
            |> Seq.collect Propulsion.CosmosStore.EquinoxCosmosStoreParser.enumStreamEvents
#if filter
            |> Seq.filter (fun e -> e.stream |> FsCodec.StreamName.toString |> filterByStreamName)
#endif
        let pipeline =
            use observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, mapToStreamItems)
            Propulsion.CosmosStore.CosmosStoreSource.Run(Log.Logger, monitored, leases, processorName, observer, startFromTail, ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
        sink, pipeline
#endif // !kafkaEventSpans

let run args = async {
#if (!kafkaEventSpans)
    let sink, pipeline = build args
    pipeline |> Async.Start
#else
    let sink = build args
#endif
    return! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
#if (!kafkaEventSpans)
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose, changeFeedProcessorVerbose=args.CfpVerbose).CreateLogger()
#else
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
#endif
            try run args |> Async.RunSynchronously
                0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
