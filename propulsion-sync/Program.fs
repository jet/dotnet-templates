module SyncTemplate.Program

open Equinox.Cosmos
open Equinox.EventStore
open Propulsion.Cosmos
open Propulsion.EventStore
#if kafka
open Propulsion.Kafka
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

    let load () =
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

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-c"; Unique>]   MaxConnections of int
        | [<AltCommandLine "-s"; Unique>]   MaxSubmit of int

        | [<AltCommandLine "-S"; Unique>]   LocalSeq
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose

        | [<AltCommandLine "-e">]           CategoryBlacklist of string
        | [<AltCommandLine "-i">]           CategoryWhitelist of string

        | [<CliPrefix(CliPrefix.None); AltCommandLine "es"; Unique(*ExactlyOnce is not supported*); Last>] SrcEs of ParseResults<EsSourceParameters>
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent writes to target permitted. Default: 512."
                | MaxConnections _ ->       "size of Sink connection pool to maintain. Default: 1."
                | MaxSubmit _ ->            "maximum number of batches to submit concurrently. Default: 8."

                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | Verbose ->                "request Verbose Logging. Default: off"
                | CfpVerbose ->             "request Verbose Change Feed Processor Logging. Default: off"

                | CategoryBlacklist _ ->    "category whitelist"
                | CategoryWhitelist _ ->    "category blacklist"

                | SrcCosmos _ ->            "Cosmos input parameters."
                | SrcEs _ ->                "EventStore input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead, 2048)
        member __.MaxWriters =              a.GetResult(MaxWriters, 512)
        member __.MaxConnections =          a.GetResult(MaxConnections, 1)
        member __.MaybeSeqEndpoint =        if a.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.MaxSubmit =               a.GetResult(MaxSubmit, 8)

        member __.Verbose =                 a.Contains Parameters.Verbose
        member __.CfpVerbose =              a.Contains CfpVerbose
        member val Source : Choice<CosmosSourceArguments, EsSourceArguments> =
            match a.TryGetSubCommand() with
            | Some (SrcCosmos cosmos) -> Choice1Of2 (CosmosSourceArguments cosmos)
            | Some (SrcEs es) -> Choice2Of2 (EsSourceArguments es)
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")

        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member __.StateInterval =           TimeSpan.FromMinutes 5.
        member __.CategoryFilterFunction(?excludeLong, ?longOnly): string -> bool =
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

        member __.Sink : Choice<CosmosSinkArguments, EsSinkArguments> =
            match __.Source with
            | Choice1Of2 cosmos -> cosmos.Sink
            | Choice2Of2 es -> Choice1Of2 es.Sink
        member x.SourceParams() : Choice<_, _*ReaderSpec> =
            match x.Source with
            | Choice1Of2 srcC ->
                let disco, db =
                    match srcC.Sink with
                    | Choice1Of2 dstC ->
                        match srcC.LeaseContainer, dstC.LeaseContainer with
                        | None, None ->     srcC.Discovery, { database = srcC.Database; container = srcC.Container + "-aux" }
                        | Some sc, None ->  srcC.Discovery, { database = srcC.Database; container = sc }
                        | None, Some dc ->  dstC.Discovery, { database = dstC.Database; container = dc }
                        | Some _, Some _ -> raise (MissingArg "LeaseContainerSource and LeaseContainerDestination are mutually exclusive - can only store in one database")
                    | Choice2Of2 _dstE ->
                        let lc = match srcC.LeaseContainer with Some sc -> sc | None -> srcC.Container + "-aux"
                        srcC.Discovery, { database = srcC.Database; container = lc }
                Log.Information("Syncing... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxWriters, x.MaxReadAhead)
                Log.Information("Monitoring Group {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                    x.ConsumerGroupName, db.database, db.container, srcC.MaxDocuments)
                if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
                srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
                Choice1Of2 (srcC, (disco, db, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency))
            | Choice2Of2 srcE ->
                let startPos = srcE.StartPos
                Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Container {container}",
                    x.ConsumerGroupName, startPos, srcE.ForceRestart, srcE.Sink.Database, srcE.Sink.Container)
                Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                    srcE.MinBatchSize, srcE.StartingBatchSize, x.MaxReadAhead)
                Choice2Of2 (srcE,
                    {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                        forceRestart = srcE.ForceRestart
                        batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = srcE.StreamReaders })
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

        | [<CliPrefix(CliPrefix.None); AltCommandLine "es"; Unique(*ExactlyOnce is not supported*); Last>] DstEs of ParseResults<EsSinkParameters>
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] DstCosmos of ParseResults<CosmosSinkParameters>
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

                | DstEs _ ->                "EventStore Sink parameters."
                | DstCosmos _ ->            "CosmosDb Sink parameters."
    and CosmosSourceArguments(a : ParseResults<CosmosSourceParameters>) =
        member __.FromTail =                a.Contains CosmosSourceParameters.FromTail
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.LeaseContainer =          a.TryGetResult CosmosSourceParameters.LeaseContainer

        member __.Mode =                    a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult CosmosSourceParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosSourceParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.GetResult CosmosSourceParameters.Container
        member __.Timeout =                 a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSourceParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.MonitoringParams() =
            let (Discovery.UriAndKey (endpointUri, _)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let c = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, c

        member val Sink =
            match a.TryGetSubCommand() with
            | Some (DstCosmos cosmos) -> Choice1Of2 (CosmosSinkArguments cosmos)
            | Some (DstEs es) -> Choice2Of2 (EsSinkArguments es)
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Sink")
    and [<NoEquality; NoComparison>] EsSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-g"; Unique>]   Gorge of int
        | [<AltCommandLine "-i"; Unique>]   StreamReaders of int
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

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Es of ParseResults<EsSinkParameters>
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSinkParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "Start the processing from the Tail"
                | Gorge _ ->                "Request Parallel readers phase during initial catchup, running one chunk (256MB) apart. Default: off"
                | StreamReaders _ ->        "number of concurrent readers that will fetch a missing stream when in tailing mode. Default: 1. TODO: IMPLEMENT!"
                | Tail _ ->                 "attempt to read from tail at specified interval in Seconds. Default: 1"
                | ForceRestart _ ->         "Forget the current committed position; start from (and commit) specified position. Default: start from specified position or resume from committed."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | MinBatchSize _ ->         "minimum item count to drop down to in reaction to read failures. Default: 512"
                | Position _ ->             "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"

                | Verbose ->                "Include low level Store logging."
                | Tcp ->                    "Request connecting direct to a TCP/IP endpoint. Default: Use Clustered mode with Gossip-driven discovery (unless environment variable EQUINOX_ES_TCP specifies 'true')."
                | Host _ ->                 "TCP mode: specify a hostname to connect to directly. Clustered mode: use Gossip protocol against all A records returned from DNS query. (optional if environment variable EQUINOX_ES_HOST specified)"
                | Port _ ->                 "specify a custom port. Uses value of environment variable EQUINOX_ES_PORT if specified. Defaults for Cluster and Direct TCP/IP mode are 30778 and 1113 respectively."
                | Username _ ->             "specify a username. (optional if environment variable EQUINOX_ES_USERNAME specified)"
                | Password _ ->             "specify a Password. (optional if environment variable EQUINOX_ES_PASSWORD specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds. Default: 1.5."

                | Cosmos _ ->               "CosmosDb Sink parameters."
                | Es _ ->                   "EventStore Sink parameters."
    and EsSourceArguments(a : ParseResults<EsSourceParameters>) =
        member __.Gorge =                   a.TryGetResult Gorge
        member __.StreamReaders =           a.GetResult(StreamReaders, 1)
        member __.TailInterval =            a.GetResult(Tail, 1.) |> TimeSpan.FromSeconds
        member __.ForceRestart =            a.Contains ForceRestart
        member __.StartingBatchSize =       a.GetResult(BatchSize, 4096)
        member __.MinBatchSize =            a.GetResult(MinBatchSize, 512)
        member __.StartPos =
            match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent, a.Contains FromTail with
            | Some p, _, _, _ ->            Absolute p
            | _, Some c, _, _ ->            StartPos.Chunk c
            | _, _, Some p, _ ->            Percentage p
            | None, None, None, true ->     StartPos.TailOrCheckpoint
            | None, None, None, _ ->        StartPos.StartOrCheckpoint

        member __.Discovery =
            match __.Tcp, __.Port with
            | false, None ->   Discovery.GossipDns            __.Host
            | false, Some p -> Discovery.GossipDnsCustomPort (__.Host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", __.Host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", __.Host, p).Uri)
        member __.Tcp =
            a.Contains EsSourceParameters.Tcp
            || EnvVar.tryGet "EQUINOX_ES_TCP" |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))
        member __.Port =                    match a.TryGetResult EsSourceParameters.Port with Some x -> Some x | None -> EnvVar.tryGet "EQUINOX_ES_PORT" |> Option.map int
        member __.Host =                    a.TryGetResult EsSourceParameters.Host     |> defaultWithEnvVar "EQUINOX_ES_HOST"     "Host"
        member __.User =                    a.TryGetResult EsSourceParameters.Username |> defaultWithEnvVar "EQUINOX_ES_USERNAME" "Username"
        member __.Password =                a.TryGetResult EsSourceParameters.Password |> defaultWithEnvVar "EQUINOX_ES_PASSWORD" "Password"
        member __.Retries =                 a.GetResult(EsSourceParameters.Retries, 3)
        member __.Timeout =                 a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member __.Heartbeat =               a.GetResult(EsSourceParameters.HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
        member x.Connect(log: ILogger, storeLog: ILogger, appName, connectionStrategy) =
            let discovery = x.Discovery
            let s (x : TimeSpan) = x.TotalSeconds
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, s x.Heartbeat, s x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Establish(appName, discovery, connectionStrategy)
        member __.CheckpointInterval =  TimeSpan.FromHours 1.

        member val Sink =
            match a.TryGetSubCommand() with
            | Some (Cosmos cosmos) -> CosmosSinkArguments cosmos
            | _ -> raise (MissingArg "Must specify cosmos for Sink if source is `es`")
    and [<NoEquality; NoComparison>] CosmosSinkParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
#if kafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<KafkaSinkParameters>
#endif
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a Container name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 0."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
#if kafka
                | Kafka _ ->                "specify Kafka target for non-Synced categories. Default: None."
#endif
    and CosmosSinkArguments(a : ParseResults<CosmosSinkParameters>) =
        member __.Mode =                    a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.TryGetResult Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member __.LeaseContainer =          a.TryGetResult LeaseContainer
        member __.Timeout =                 a.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSinkParameters.Retries, 0)
        member __.MaxRetryWaitTime =        a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        /// Connect with the provided parameters and/or environment variables
        member x.Connect
            /// Connection/Client identifier for logging purposes
            appName connIndex : Async<Equinox.Cosmos.Connection> =
            let (Discovery.UriAndKey (endpointUri, _masterKey)) as discovery = x.Discovery
            Log.Information("Destination CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Destination CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let c = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c.Connect(sprintf "App=%s Conn=%d" appName connIndex, discovery)
#if kafka
        member val KafkaSink =
            match a.TryGetSubCommand() with
            | Some (Kafka kafka) -> Some (KafkaSinkArguments kafka)
            | _ -> None
#endif
    and [<NoEquality; NoComparison>] EsSinkParameters =
        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-T">]           Tcp
        | [<AltCommandLine "-h">]           Host of string
        | [<AltCommandLine "-x">]           Port of int
        | [<AltCommandLine "-u">]           Username of string
        | [<AltCommandLine "-p">]           Password of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-oh">]          HeartbeatTimeout of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                "Include low level Store logging."
                | Tcp ->                    "Request connecting direct to a TCP/IP endpoint. Default: Use Clustered mode with Gossip-driven discovery (unless environment variable EQUINOX_ES_TCP specifies 'true')."
                | Host _ ->                 "TCP mode: specify a hostname to connect to directly. Clustered mode: use Gossip protocol against all A records returned from DNS query. (optional if environment variable EQUINOX_ES_HOST specified)"
                | Port _ ->                 "specify a custom port. Uses value of environment variable EQUINOX_ES_PORT if specified. Defaults for Cluster and Direct TCP/IP mode are 30778 and 1113 respectively."
                | Username _ ->             "specify a username. (optional if environment variable EQUINOX_ES_USERNAME specified)"
                | Password _ ->             "specify a password. (optional if environment variable EQUINOX_ES_PASSWORD specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds. Default: 1.5."
    and EsSinkArguments(a : ParseResults<EsSinkParameters>) =
        member __.Discovery =
            match __.Tcp, __.Port with
            | false, None ->   Discovery.GossipDns            __.Host
            | false, Some p -> Discovery.GossipDnsCustomPort (__.Host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", __.Host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", __.Host, p).Uri)
        member __.Tcp =
            a.Contains EsSinkParameters.Tcp
            || EnvVar.tryGet "EQUINOX_ES_TCP" |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))
        member __.Port =                    match a.TryGetResult Port with Some x -> Some x | None -> EnvVar.tryGet "EQUINOX_ES_PORT" |> Option.map int
        member __.Host =                    a.TryGetResult Host     |> defaultWithEnvVar "EQUINOX_ES_HOST"     "Host"
        member __.User =                    a.TryGetResult Username |> defaultWithEnvVar "EQUINOX_ES_USERNAME" "Username"
        member __.Password =                a.TryGetResult Password |> defaultWithEnvVar "EQUINOX_ES_PASSWORD" "Password"
        member __.Retries =                 a.GetResult(Retries, 3)
        member __.Timeout =                 a.GetResult(Timeout, 20.) |> TimeSpan.FromSeconds
        member __.Heartbeat =               a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
        member x.Connect(log: ILogger, storeLog: ILogger, connectionStrategy, appName, connIndex) =
            let discovery = x.Discovery
            let s (x : TimeSpan) = x.TotalSeconds
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, s x.Heartbeat, s x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Establish(appName, discovery, connectionStrategy)
#if kafka
    and [<NoEquality; NoComparison>] KafkaSinkParameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        | [<AltCommandLine "-p"; Unique>]   Producers of int
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)."
                | Producers _ ->            "specify number of Kafka Producer instances to use. Default: 1."
    and KafkaSinkArguments(a : ParseResults<KafkaSinkParameters>) =
        member __.Broker =                  a.TryGetResult Broker |> defaultWithEnvVar "PROPULSION_KAFKA_BROKER" "Broker"
        member __.Topic =                   a.TryGetResult Topic  |> defaultWithEnvVar "PROPULSION_KAFKA_TOPIC"  "Topic"
        member __.Producers =               a.GetResult(Producers, 1)
        member x.BuildTargetParams() =      x.Broker, x.Topic, x.Producers
#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        parser.ParseCommandLine argv |> Arguments

open Serilog.Events

// Application logic assumes the global `Serilog.Log` is initialized _immediately_ after a successful ArgumentParser.ParseCommandline
type Logging() =

    static member Initialize(configure) =
        let loggerConfiguration : LoggerConfiguration = LoggerConfiguration() |> configure
        Log.Logger <- loggerConfiguration.CreateLogger()

    static member Configure(configuration : LoggerConfiguration, verbose, changeFeedProcessorVerbose, ?maybeSeqEndpoint) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> c.ConfigureChangeFeedProcessorLogging(changeFeedProcessorVerbose)
        |> fun c -> let ingesterLevel = if changeFeedProcessorVerbose then LogEventLevel.Debug else LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamSchedulingEngine>.FullName, ingesterLevel)
        |> fun c -> if verbose then c.MinimumLevel.Debug() else c
        |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.Cosmos.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Checkpoint.CheckpointSeries>.FullName, LogEventLevel.Information)
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                    let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                        a.Logger(fun l ->
                            l.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
                             .WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink()) |> ignore) |> ignore
                        a.Logger(fun l ->
                            let isEqx = Filters.Matching.FromSource<Equinox.Cosmos.Core.Context>().Invoke
                            let isWriterA = Filters.Matching.FromSource<Propulsion.EventStore.Internal.Writer.Result>().Invoke
                            let isWriterB = Filters.Matching.FromSource<Propulsion.Cosmos.Internal.Writer.Result>().Invoke
                            let isCp = Filters.Matching.FromSource<Propulsion.EventStore.Checkpoint.CheckpointSeries>().Invoke
                            let l =
                                if changeFeedProcessorVerbose then l
                                else l.Filter.ByExcluding(fun x -> isEqx x || isWriterA x || isWriterB x || isCp x)
                            l.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                            |> ignore) |> ignore
                    c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)
        |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)

 //#if marveleqx
[<RequireQualifiedAccess>]
module EventV0Parser =
    open Newtonsoft.Json

    /// A single Domain Event as Written by internal Equinox versions
    type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
        EventV0 =
        {   /// CosmosDB-mandated Partition Key, must be maintained within the document
            s: string // "{streamName}"

            /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
            c: DateTimeOffset // ISO 8601

            /// The Case (Event Type); used to drive deserialization
            t: string // required

            /// 'i' value for the Event
            i: int64 // {index}

            /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for CosmosDB
            [<JsonConverter(typeof<FsCodec.NewtonsoftJson.VerbatimUtf8JsonConverter>)>]
            d: byte[] }
        interface FsCodec.ITimelineEvent<byte[]> with
            member x.Index = x.i
            member x.IsUnfold = false
            member x.EventType = x.t
            member x.Data = x.d
            member __.Meta = null
            member __.EventId = Guid.Empty
            member x.Timestamp = x.c
            member __.CorrelationId = null
            member __.CausationId = null
            member __.Context = null

    type Microsoft.Azure.Documents.Document with
        member document.Cast<'T>() =
            let tmp = new Microsoft.Azure.Documents.Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")

    /// We assume all Documents represent Events laid out as above
    let parse (d : Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> =
        let e = d.Cast<EventV0>()
        { stream = FsCodec.StreamName.parse e.s; event = e } : _

let transformV0 catFilter (v0SchemaDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    let parsed = EventV0Parser.parse v0SchemaDocument
    let (FsCodec.StreamName.CategoryAndId (cat, _)) = parsed.stream
    if catFilter cat then
        yield parsed }
//#else
let transformOrFilter catFilter (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    for { stream = FsCodec.StreamName.CategoryAndId (cat, _) } as e in EquinoxCosmosParser.enumStreamEvents changeFeedDocument do
        // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
        if catFilter cat then
            yield e }
//#endif

let [<Literal>] AppName = "SyncTemplate"

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

type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Sync.Stats<unit>(log, statsInterval, stateInterval)

    override __.HandleOk(()) = ()
    override __.HandleExn exn = log.Information(exn, "Unhandled")

let build (args : Args.Arguments, log, storeLog : ILogger) =
    let maybeDstCosmos, sink, streamFilter =
        match args.Sink with
        | Choice1Of2 cosmos ->
            let containers = Containers(cosmos.Database, cosmos.Container)
            let connect connIndex = async {
                let! c = cosmos.Connect AppName connIndex
                let lfc = storeLog.ForContext("ConnId", connIndex)
                return c, Equinox.Cosmos.Core.Context(c, containers, lfc) }
            let all = Array.init args.MaxConnections connect |> Async.Parallel |> Async.RunSynchronously
            let mainConn, targets = Equinox.Cosmos.Gateway(fst all.[0], Equinox.Cosmos.BatchingPolicy()), Array.map snd all
            let sink, streamFilter =
#if kafka
                let maxEvents, maxBytes = 100_000, 900_000
                match cosmos.KafkaSink with
                | Some kafka ->
                    let (broker, topic, producers) = kafka.BuildTargetParams()
                    let render (stream: FsCodec.StreamName, span: Propulsion.Streams.StreamSpan<_>) = async {
                        let value =
                            span
                            |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
                            |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
                        return FsCodec.StreamName.toString stream, value }
                    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, topic, degreeOfParallelism=producers)
                    let stats = Stats(Log.Logger, args.StatsInterval, args.StateInterval)
                    StreamsProducerSink.Start(
                        Log.Logger, args.MaxReadAhead, args.MaxWriters, render, producer, stats, args.StatsInterval, maxBytes=maxBytes, maxEvents=maxEvents),
                    args.CategoryFilterFunction(longOnly=true)
                | None ->
#endif
                CosmosSink.Start(log, args.MaxReadAhead, targets, args.MaxWriters, args.StatsInterval, args.StateInterval, maxSubmissionsPerPartition=args.MaxSubmit),
                args.CategoryFilterFunction(excludeLong=true)
            Some (mainConn, containers), sink, streamFilter
        | Choice2Of2 es ->
            let connect connIndex = async {
                let lfc = storeLog.ForContext("ConnId", connIndex)
                let! c = es.Connect(log, lfc, ConnectionStrategy.ClusterSingle NodePreference.Master, AppName, connIndex)
                return Context(c, BatchingPolicy(Int32.MaxValue)) }
            let targets = Array.init args.MaxConnections (string >> connect) |> Async.Parallel |> Async.RunSynchronously
            let sink = EventStoreSink.Start(log, storeLog, args.MaxReadAhead, targets, args.MaxWriters, args.StatsInterval, args.StateInterval, maxSubmissionsPerPartition=args.MaxSubmit)
            None, sink, args.CategoryFilterFunction()
    match args.SourceParams() with
    | Choice1Of2 (srcC, (auxDiscovery, aux, leaseId, startFromTail, maxDocuments, lagFrequency)) ->
        let discovery, source, connector = srcC.MonitoringParams()
#if marveleqx
        let createObserver () = CosmosSource.CreateObserver(log, sink.StartIngester, Seq.collect (transformV0 streamFilter))
#else
        let createObserver () = CosmosSource.CreateObserver(log, sink.StartIngester, Seq.collect (transformOrFilter streamFilter))
#endif
        let runPipeline =
            CosmosSource.Run(log, connector.CreateClient(AppName, discovery), source, aux,
                leaseId, startFromTail, createObserver,
                ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency, auxClient=connector.CreateClient(AppName, auxDiscovery))
        sink, runPipeline
    | Choice2Of2 (srcE, spec) ->
        match maybeDstCosmos with
        | None -> failwith "ES->ES checkpointing E_NOTIMPL"
        | Some (mainConn, containers) ->

        let context = Equinox.Cosmos.Context(mainConn, containers)
        let cache = Equinox.Cache(AppName, sizeMb=1)
        let checkpoints = Checkpoints.Cosmos.create spec.groupName (context, cache)

        let withNullData (e : FsCodec.ITimelineEvent<_>) : FsCodec.ITimelineEvent<_> =
            FsCodec.Core.TimelineEvent.Create(e.Index, e.EventType, null, e.Meta, timestamp=e.Timestamp) :> _
        let tryMapEvent streamFilter (x : EventStore.ClientAPI.ResolvedEvent) =
            match x.Event with
            | e when not e.IsJson || e.EventStreamId.StartsWith "$"
                || not (streamFilter e.EventStreamId)  -> None
            | PropulsionStreamEvent e ->
                if Propulsion.EventStore.Reader.payloadBytes x > 1_000_000 then
                    Log.Error("replacing {stream} event index {index} with `null` Data due to length of {len}MB",
                        e.stream, e.event.Index, Propulsion.EventStore.Reader.mb e.event.Data.Length)
                    Some { e with event = withNullData e.event }
                else Some e
        let connect () =
            let c = srcE.Connect(log, log, AppName, ConnectionStrategy.ClusterSingle NodePreference.PreferSlave) |> Async.RunSynchronously
            c.ReadConnection
        let runPipeline =
            EventStoreSource.Run(
                log, sink, checkpoints, connect, spec, tryMapEvent streamFilter,
                args.MaxReadAhead, args.StatsInterval)
        sink, runPipeline

let run args = async {
    let log, storeLog = Log.ForContext<Propulsion.Streams.Scheduling.StreamSchedulingEngine>(), Log.ForContext<Equinox.Cosmos.Core.Context>()
    let sink, pipeline = build (args, log, storeLog)
    pipeline |> Async.Start
    return! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
        try Logging.Initialize(fun c -> Logging.Configure(c, args.Verbose, args.CfpVerbose, ?maybeSeqEndpoint = args.MaybeSeqEndpoint))
            try Configuration.load ()
                run args |> Async.RunSynchronously
                0
            with e when not (e :? Args.MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
