module ReactorTemplate.Program

//#if (multiSource || !blank)
open Equinox.Cosmos
//#endif
//#if (!kafkaEventSpans)
open Propulsion.Cosmos
//#if multiSource
open Propulsion.EventStore
//#endif
//#endif
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
    let private isEnvVarTrue varName =
         EnvVar.tryGet varName |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))
    let private seconds (x : TimeSpan) = x.TotalSeconds
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
            member a.Usage =
                match a with
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
    and Arguments(a : ParseResults<Parameters>) =
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.Verbose =                 a.Contains Parameters.Verbose
//#if (!kafkaEventSpans)
        member __.CfpVerbose =              a.Contains CfpVerbose
//#endif
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead, 16)
        member __.MaxConcurrentStreams =    a.GetResult(MaxWriters, 8)
        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member __.StateInterval =           TimeSpan.FromMinutes 5.
//#if filter
        member __.FilterFunction(?excludeLong, ?longOnly): string -> bool =
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
            | Some (Parameters.Cosmos cosmos) -> CosmosSourceArguments cosmos
            | _ -> raise (MissingArg "Must specify cosmos for Src")
        member x.SourceParams() =
                let srcC = x.Source
#endif
//#if (!kafkaEventSpans)
//#if (!changeFeedOnly)
        member val Source : Choice<EsSourceArguments, CosmosSourceArguments> =
            match a.TryGetSubCommand() with
            | Some (Es es) -> Choice1Of2 (EsSourceArguments es)
            | Some (Parameters.Cosmos cosmos) -> Choice2Of2 (CosmosSourceArguments cosmos)
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")
        member x.SourceParams() : Choice<EsSourceArguments*CosmosArguments*ReaderSpec, CosmosSourceArguments*_> =
            match x.Source with
            | Choice1Of2 srcE ->
                let startPos, cosmos = srcE.StartPos, srcE.Cosmos
                Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Container {container}",
                    x.ConsumerGroupName, startPos, srcE.ForceRestart, cosmos.Database, cosmos.Container)
                Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                    srcE.MinBatchSize, srcE.StartingBatchSize, x.MaxReadAhead)
                Choice1Of2 (srcE, cosmos,
                    {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                        forceRestart = srcE.ForceRestart
                        batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = 0 })
            | Choice2Of2 srcC ->
//#endif // !changeFeedOnly
                let auxColl =
                    match srcC.LeaseContainer with
                    | None ->     { database = srcC.Database; container = srcC.Container + "-aux" }
                    | Some sc ->  { database = srcC.Database; container = sc }
                Log.Information("Max read backlog: {maxReadAhead}", x.MaxReadAhead)
                Log.Information("Processing Lease {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                    x.ConsumerGroupName, auxColl.database, auxColl.container, srcC.MaxDocuments)
                if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
                srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
#if changeFeedOnly
                (srcC, (auxColl, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency))
#else
                Choice2Of2 (srcC, (auxColl, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency))
#endif
//#endif // kafkaEventSpans
#if kafkaEventSpans
        member val Source : KafkaSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Kafka kafka) -> KafkaSourceArguments kafka
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
    and KafkaSourceArguments(a : ParseResults<KafkaSourceParameters>) =
        member __.Broker =                  a.TryGetResult KafkaSourceParameters.Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker"
        member __.Topic =                   a.TryGetResult KafkaSourceParameters.Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
        member __.MaxInFlightBytes =        a.GetResult(MaxInflightMb, 10.) * 1024. * 1024. |> int64
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map System.TimeSpan.FromMinutes
        member x.BuildSourceParams() =      x.Broker, x.Topic
#if (kafka && blank)
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (KafkaSourceParameters.Kafka kafka) -> KafkaSinkArguments kafka
            | _ -> raise (MissingArg "Must specify kafka arguments")
#else
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (KafkaSourceParameters.Cosmos cosmos) -> CosmosArguments cosmos
            | _ -> raise (MissingArg "Must specify cosmos details")
#endif
#else
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
    and CosmosSourceArguments(a : ParseResults<CosmosSourceParameters>) =
        member __.FromTail =                a.Contains CosmosSourceParameters.FromTail
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.LeaseContainer =          a.TryGetResult CosmosSourceParameters.LeaseContainer

        member __.Mode =                    a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Equinox.Cosmos.Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult CosmosSourceParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosSourceParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.GetResult CosmosSourceParameters.Container
        member __.Timeout =                 a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSourceParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let (Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, connector
#if (!multiSource && kafka && blank)
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Kafka kafka) -> KafkaSinkArguments kafka
            | _ -> raise (MissingArg "Must specify `kafka` arguments")
#else
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Cosmos cosmos) -> CosmosArguments cosmos
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
    and EsSourceArguments(a : ParseResults<EsSourceParameters>) =
        let discovery (host, port, tcp) =
            match tcp, port with
            | false, None ->   Discovery.GossipDns            host
            | false, Some p -> Discovery.GossipDnsCustomPort (host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", host).Uri)
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
        member __.ProjTcp =                 a.Contains ProjTcp || isEnvVarTrue "EQUINOX_ES_PROJ_TCP"
        member __.ProjPort =                match a.TryGetResult ProjPort with
                                            | Some x -> Some x
                                            | None -> EnvVar.tryGet "EQUINOX_ES_PROJ_PORT" |> Option.map int |> Option.orElseWith (fun () -> __.Port) 
        member __.ProjHost =                match a.TryGetResult ProjHost with
                                            | Some x -> x
                                            | None -> EnvVar.tryGet "EQUINOX_ES_PROJ_HOST"      |> Option.defaultWith (fun () -> __.Host)
        member __.ProjUser =                match a.TryGetResult ProjUsername with
                                            | Some x -> x
                                            | None -> EnvVar.tryGet  "EQUINOX_ES_PROJ_USERNAME" |> Option.defaultWith (fun () -> __.User)
        member __.ProjPassword =            match a.TryGetResult ProjPassword with
                                            | Some x -> x
                                            | None -> EnvVar.tryGet  "EQUINOX_ES_PROJ_PASSWORD" |> Option.defaultWith (fun () -> __.Password)
        member __.Retries =                 a.GetResult(EsSourceParameters.Retries, 3)
        member __.Timeout =                 a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member __.Heartbeat =               a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
        
        member x.ConnectProj(log: ILogger, storeLog: ILogger, appName, nodePreference) =
            let discovery = discovery (x.ProjHost, x.ProjPort, x.ProjTcp)
            log.ForContext("projHost", x.ProjHost).ForContext("projPort", x.ProjPort)
                .Information("Projection EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, seconds x.Heartbeat, seconds x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.ProjUser, x.ProjPassword, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Connect(appName + "-Proj", discovery, nodePreference) |> Async.RunSynchronously
        
        member x.Connect(log: ILogger, storeLog: ILogger, appName, connectionStrategy) =
            let discovery = discovery (x.Host, x.Port, x.Tcp)
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, seconds x.Heartbeat, seconds x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Establish(appName, discovery, connectionStrategy) |> Async.RunSynchronously

        member __.CheckpointInterval =  TimeSpan.FromHours 1.
        member val Cosmos : CosmosArguments =
            match a.TryGetSubCommand() with
            | Some (EsSourceParameters.Cosmos cosmos) -> CosmosArguments cosmos
            | _ -> raise (MissingArg "Must specify `cosmos` checkpoint store when source is `es`")
//#endif
#endif
//#if (multiSource || !(blank && kafka))
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
//#if kafka
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Kafka of ParseResults<KafkaSinkParameters>
//#endif
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
//#if kafka
                | Kafka _ ->                "Kafka Sink parameters."
//#endif
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
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, x.Database, x.Container, connector
//#if kafka
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (CosmosParameters.Kafka kafka) -> KafkaSinkArguments kafka
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
    and KafkaSinkArguments(a : ParseResults<KafkaSinkParameters>) =
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
// Application logic assumes the global `Serilog.Log` is initialized _immediately_ after a successful Args.parse
module Logging =

#if (!kafkaEventSpans)
    let initialize verbose changeFeedProcessorVerbose =
#else
    let initialize verbose =
#endif
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
#if (!kafkaEventSpans)
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

let [<Literal>] AppName = "ReactorTemplate"

//#if multiSource
#if (!kafkaEventSpans)
//#if (!changeFeedOnly)
module Checkpoints =

    // In this implementation, we keep the checkpoints in Cosmos when consuming from EventStore
    module Cosmos =

        let codec = FsCodec.NewtonsoftJson.Codec.Create<Checkpoint.Events.Event>()
        let access = Equinox.Cosmos.AccessStrategy.Custom (Checkpoint.Fold.isOrigin, Checkpoint.Fold.transmute)
        let create groupName (context, cache) =
            let caching = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
            let resolver = Equinox.Cosmos.Resolver(context, codec, Checkpoint.Fold.fold, Checkpoint.Fold.initial, caching, access)
            let resolve streamName = resolver.Resolve(streamName, Equinox.AllowStale)
            Checkpoint.CheckpointSeries(groupName, resolve)

//#endif
#endif
module EventStoreContext =

    let create connection =
        Equinox.EventStore.Context(connection, Equinox.EventStore.BatchingPolicy(maxBatchSize=500))

//#endif
#if (!kafkaEventSpans)
module CosmosContext =

    let create (connector : Equinox.Cosmos.Connector) discovery (database, container) =
        let connection = connector.Connect(AppName, discovery) |> Async.RunSynchronously
        Equinox.Cosmos.Context(connection, database, container)

#endif
let build (args : Args.Arguments) =
#if (!kafkaEventSpans)
//#if (!changeFeedOnly)
    match args.SourceParams() with
    | Choice1Of2 (srcE, cosmos, spec) ->
        let connectEs () = srcE.Connect(Log.Logger, Log.Logger, AppName, Equinox.EventStore.ConnectionStrategy.ClusterSingle Equinox.EventStore.NodePreference.Master)
        let connectProjEs () = srcE.ConnectProj(Log.Logger, Log.Logger, AppName, Equinox.EventStore.NodePreference.PreferSlave)
        let (discovery, database, container, connector) = cosmos.BuildConnectionDetails()

        let context = CosmosContext.create connector discovery (database, container)
        let cache = Equinox.Cache(AppName, sizeMb=10)

        let checkpoints = Checkpoints.Cosmos.create spec.groupName (context, cache)
#if kafka
        let (broker, topic) = srcE.Cosmos.Sink.BuildTargetParams()
        let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic)
        let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
            producer.ProduceAsync(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)
#if blank
        let handle = Handler.handleStreamEvents (Handler.handle produceSummary)
#else
        let esConn = connectEs ()
        let srcCache = Equinox.Cache(AppName, sizeMb=10)
        let srcService = Todo.EventStore.create (EventStoreContext.create esConn, srcCache)
        let handle = Handler.handleStreamEvents (Handler.handle srcService produceSummary)
#endif
        let stats = Handler.Stats(Log.Logger, args.StatsInterval, args.StateInterval, logExternalStats=producer.DumpStats)
        let sink =
#if (kafka && !blank)
             Propulsion.Streams.Sync.StreamsSync.Start(
                 Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle,
                 stats, projectorStatsInterval=args.StatsInterval)
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
    | Choice2Of2 (source, (aux, leaseId, startFromTail, maxDocuments, lagFrequency)) ->
//#endif // changeFeedOnly
#if changeFeedOnly
        let (source, (aux, leaseId, startFromTail, maxDocuments, lagFrequency)) = args.SourceParams()
#endif
        let monitoredDiscovery, monitored, monitoredConnector = source.BuildConnectionDetails()
//#if (!(kafka && blank))
        let cosmos = source.Cosmos
        let (discovery, database, container, connector) = cosmos.BuildConnectionDetails()
//#endif
#else // !kafkaEventSpans -> wire up consumption from Kafka, with auxiliary `cosmos` store
        let source = args.Source
//#if (!(kafka && blank))
        let cosmos = source.Cosmos
        let (discovery, database, container, connector) = cosmos.BuildConnectionDetails()
//#endif
        let consumerConfig =
            FsKafka.KafkaConsumerConfig.Create(
                AppName, source.Broker, [source.Topic], args.ConsumerGroupName,
                maxInFlightBytes = source.MaxInFlightBytes, ?statisticsInterval = source.LagFrequency)
#endif // kafkaEventSpans

#if kafka
#if (blank && !multiSource)
        let (broker, topic) = source.Sink.BuildTargetParams()
#else
        let (broker, topic) = source.Cosmos.Sink.BuildTargetParams()
#endif
        let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic)
        let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
            producer.ProduceAsync(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)
#if blank
        let handle = Handler.handle produceSummary
#else
        let context = CosmosContext.create connector discovery (database, container)
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
        let context = CosmosContext.create connector discovery (database, container)
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

        let parseStreamEvents(KeyValue (_streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
            Propulsion.Codec.NewtonsoftJson.RenderedSpan.parse spanJson
#if filter
            |> Seq.filter (fun e -> e.stream |> FsCodec.StreamName.toString |> filterByStreamName)
#endif
        Propulsion.Kafka.StreamsConsumer.Start(Log.Logger, consumerConfig, parseStreamEvents, handle, args.MaxConcurrentStreams, stats=stats,  pipelineStatsInterval=args.StatsInterval)
#else // !kafkaEventSpans => Default consumption, from CosmosDb
#if (kafka && !blank)
        let sink =
            Propulsion.Streams.Sync.StreamsSync.Start(
                 Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats,
                 projectorStatsInterval = args.StatsInterval)
#else
        let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats, args.StatsInterval)
#endif

        let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
            // TODO: customize parsing to events if source is not an Equinox Container
            docs
            |> Seq.collect EquinoxCosmosParser.enumStreamEvents
#if filter
            |> Seq.filter (fun e -> e.stream |> FsCodec.StreamName.toString |> filterByStreamName)
#endif
        let createObserver () = CosmosSource.CreateObserver(Log.Logger, sink.StartIngester, mapToStreamItems)
        let runPipeline =
            CosmosSource.Run(Log.Logger, monitoredConnector.CreateClient(AppName, monitoredDiscovery), monitored, aux,
                leaseId, startFromTail, createObserver,
                ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
        sink, runPipeline
#endif // !kafkaEventSpans

let run args =
#if (!kafkaEventSpans)
    let projector, runSourcePipeline = build args
    runSourcePipeline |> Async.Start
#else
    let projector = build args
#endif
    projector.AwaitCompletion() |> Async.RunSynchronously
    projector.RanToCompletion

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
#if (!kafkaEventSpans)
        try Logging.initialize args.Verbose args.CfpVerbose
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
