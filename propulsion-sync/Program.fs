module SyncTemplate.Program

open Equinox.EventStore
open Propulsion.EventStore
#if kafka
open Propulsion.Kafka
#endif
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
    member _.EventStoreHost =               get "EQUINOX_ES_HOST"
    member _.EventStoreTcp =                isTrue "EQUINOX_ES_TCP"
    member _.EventStorePort =               tryGet "EQUINOX_ES_PORT" |> Option.map int
    member _.EventStoreUsername =           get "EQUINOX_ES_USERNAME"
    member _.EventStorePassword =           get "EQUINOX_ES_PASSWORD"
#if kafka
    member _.Broker =                       get "PROPULSION_KAFKA_BROKER"
    member _.Topic =                        get "PROPULSION_KAFKA_TOPIC"
#endif

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-I"; Unique>]   StoreVerbose
        | [<AltCommandLine "-S"; Unique>]   LocalSeq
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-c"; Unique>]   MaxConnections of int
        | [<AltCommandLine "-s"; Unique>]   MaxSubmit of int
        | [<AltCommandLine "-e">]           CategoryBlacklist of string
        | [<AltCommandLine "-i">]           CategoryWhitelist of string
        | [<CliPrefix(CliPrefix.None); AltCommandLine "es"; Unique(*ExactlyOnce is not supported*); Last>] SrcEs of ParseResults<EsSourceParameters>
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                "request Verbose Logging. Default: off"
                | StoreVerbose ->           "request Verbose Ingester Logging. Default: off"
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent writes to target permitted. Default: 512."
                | MaxConnections _ ->       "size of Sink connection pool to maintain. Default: 1."
                | MaxSubmit _ ->            "maximum number of batches to submit concurrently. Default: 8."
                | CategoryBlacklist _ ->    "category whitelist"
                | CategoryWhitelist _ ->    "category blacklist"

                | SrcCosmos _ ->            "Cosmos input parameters."
                | SrcEs _ ->                "EventStore input parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val Verbose =                a.Contains Parameters.Verbose
        member val StoreVerbose =           a.Contains StoreVerbose
        member val MaybeSeqEndpoint =       if a.Contains LocalSeq then Some "http://localhost:5341" else None
        member val ProcessorName =          a.GetResult ProcessorName
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 2048)
        member val MaxWriters =             a.GetResult(MaxWriters, 512)
        member val MaxConnections =         a.GetResult(MaxConnections, 1)
        member val MaxSubmit =              a.GetResult(MaxSubmit, 8)

        member val Source : Choice<CosmosSourceArguments, EsSourceArguments> =
            match a.TryGetSubCommand() with
            | Some (SrcCosmos cosmos) -> Choice1Of2 (CosmosSourceArguments (c, cosmos))
            | Some (SrcEs es) -> Choice2Of2 (EsSourceArguments (c, es))
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")

        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member _.CategoryFilterFunction(?excludeLong, ?longOnly): string -> bool =
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

        member x.Sink : Choice<CosmosSinkArguments, EsSinkArguments> =
            match x.Source with
            | Choice1Of2 cosmos -> cosmos.Sink
            | Choice2Of2 es -> Choice1Of2 es.Sink
        member x.SourceParams() : Choice<_, _*Equinox.CosmosStore.CosmosStoreContext*ReaderSpec> =
            match x.Source with
            | Choice1Of2 srcC ->
                let leases : Microsoft.Azure.Cosmos.Container =
                    match srcC.Sink with
                    | Choice1Of2 dstC ->
                        match srcC.LeaseContainerId, dstC.LeaseContainerId with
                        | _, None ->        srcC.ConnectLeases()
                        | None, Some dc ->  dstC.ConnectLeases dc
                        | Some _, Some _ -> raise (MissingArg "LeaseContainerSource and LeaseContainerDestination are mutually exclusive - can only store in one database")
                    | Choice2Of2 _dstE ->   srcC.ConnectLeases()
                Log.Information("Syncing... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxWriters, x.MaxReadAhead)
                Log.Information("ChangeFeed {processorName} Leases Database {db} Container {container}. MaxItems limited to {maxItems}",
                    x.ProcessorName, leases.Database.Id, leases.Id, srcC.MaxItems)
                if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
                Log.Information("ChangeFeed Lag stats interval {lagS:n0}s", let f = srcC.LagFrequency in f.TotalSeconds)
                let monitored = srcC.MonitoredContainer()
                Choice1Of2 (monitored, leases, x.ProcessorName, srcC.FromTail, srcC.MaxItems, srcC.LagFrequency)
            | Choice2Of2 srcE ->
                let startPos = srcE.StartPos
                let checkpointsContext = srcE.Sink.Connect() |> Async.RunSynchronously |> CosmosStoreContext.create
                Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart})",
                    x.ProcessorName, startPos, srcE.ForceRestart)
                Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxReadAhead} uncommitted batches ahead",
                    srcE.MinBatchSize, srcE.StartingBatchSize, x.MaxReadAhead)
                Choice2Of2 (srcE, checkpointsContext,
                    {   groupName = x.ProcessorName; start = startPos; checkpointInterval = srcE.CheckpointInterval; tailInterval = srcE.TailInterval
                        forceRestart = srcE.ForceRestart
                        batchSize = srcE.StartingBatchSize; minBatchSize = srcE.MinBatchSize; gorge = srcE.Gorge; streamReaders = srcE.StreamReaders })
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-mi"; Unique>]  MaxItems of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
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
                | MaxItems _ ->             "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: 1"
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
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        let discovery =                     a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult CosmosSourceParameters.ConnectionMode
        let timeout =                       a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSourceParameters.Retries, 1)
        let maxRetryWaitTime =              a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      a.TryGetResult CosmosSourceParameters.Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val ContainerId : string =            a.GetResult CosmosSourceParameters.Container
        member x.MonitoredContainer() =     connector.ConnectMonitored(database, x.ContainerId)

        member val FromTail =               a.Contains CosmosSourceParameters.FromTail
        member val MaxItems =               a.TryGetResult MaxItems
        member val LagFrequency : TimeSpan = a.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member val LeaseContainerId =       a.TryGetResult CosmosSourceParameters.LeaseContainer
        member private _.ConnectLeases containerId = connector.CreateUninitialized(database, containerId)
        member x.ConnectLeases() =          match x.LeaseContainerId with
                                            | None ->    x.ConnectLeases(x.ContainerId + "-aux")
                                            | Some sc -> x.ConnectLeases(sc)
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (DstCosmos cosmos) -> Choice1Of2 (CosmosSinkArguments (c, cosmos))
            | Some (DstEs es) -> Choice2Of2 (EsSinkArguments (c, es))
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
    and EsSourceArguments(c : Configuration, a : ParseResults<EsSourceParameters>) =
        member val Gorge =                  a.TryGetResult Gorge
        member val StreamReaders =          a.GetResult(StreamReaders, 1)
        member val TailInterval =           a.GetResult(Tail, 1.) |> TimeSpan.FromSeconds
        member val ForceRestart =           a.Contains ForceRestart
        member val StartingBatchSize =      a.GetResult(BatchSize, 4096)
        member val MinBatchSize =           a.GetResult(MinBatchSize, 512)
        member val StartPos =
            match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent, a.Contains FromTail with
            | Some p, _, _, _ ->            Absolute p
            | _, Some c, _, _ ->            StartPos.Chunk c
            | _, _, Some p, _ ->            Percentage p
            | None, None, None, true ->     StartPos.TailOrCheckpoint
            | None, None, None, _ ->        StartPos.StartOrCheckpoint

        member x.Discovery =
            match x.Tcp, x.Port with
            | false, None ->   Discovery.GossipDns            x.Host
            | false, Some p -> Discovery.GossipDnsCustomPort (x.Host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", x.Host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", x.Host, p).Uri)
        member val Tcp =                    a.Contains EsSourceParameters.Tcp || c.EventStoreTcp
        member val Port =                   match a.TryGetResult EsSourceParameters.Port with Some x -> Some x | None -> c.EventStorePort
        member val Host =                   a.TryGetResult EsSourceParameters.Host     |> Option.defaultWith (fun () -> c.EventStoreHost)
        member val User =                   a.TryGetResult EsSourceParameters.Username |> Option.defaultWith (fun () -> c.EventStoreUsername)
        member val Password =               a.TryGetResult EsSourceParameters.Password |> Option.defaultWith (fun () -> c.EventStorePassword)
        member val Retries =                a.GetResult(EsSourceParameters.Retries, 3)
        member val Timeout =                a.GetResult(EsSourceParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member val Heartbeat =              a.GetResult(EsSourceParameters.HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
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
        member _.CheckpointInterval =   TimeSpan.FromHours 1.

        member val Sink =
            match a.TryGetSubCommand() with
            | Some (Cosmos cosmos) -> CosmosSinkArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Sink if source is `es`")
    and [<NoEquality; NoComparison>] CosmosSinkParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
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
    and CosmosSinkArguments(c : Configuration, a : ParseResults<CosmosSinkParameters>) =
        let discovery =                     a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.GetResult(ConnectionMode, Microsoft.Azure.Cosmos.ConnectionMode.Direct)
        let timeout =                       a.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSinkParameters.Retries, 0)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, mode)
        let database =                      a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let container =                     a.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        member _.Connect() =                connector.ConnectStore("Destination", database, container)

        member val LeaseContainerId =       a.TryGetResult LeaseContainer
        member _.ConnectLeases containerId = connector.CreateUninitialized(database, containerId)
#if kafka
        member val KafkaSink =
            match a.TryGetSubCommand() with
            | Some (Kafka kafka) -> Some (KafkaSinkArguments (c, kafka))
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
    and EsSinkArguments(c : Configuration, a : ParseResults<EsSinkParameters>) =
        member x.Discovery =
            match x.Tcp, x.Port with
            | false, None ->   Discovery.GossipDns            x.Host
            | false, Some p -> Discovery.GossipDnsCustomPort (x.Host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", x.Host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", x.Host, p).Uri)
        member val Tcp =                    a.Contains EsSinkParameters.Tcp || c.EventStoreTcp
        member val Port =                   match a.TryGetResult Port with Some x -> Some x | None -> c.EventStorePort
        member val Host =                   a.TryGetResult Host     |> Option.defaultWith (fun () -> c.EventStoreHost)
        member val User =                   a.TryGetResult Username |> Option.defaultWith (fun () -> c.EventStoreUsername)
        member val Password =               a.TryGetResult Password |> Option.defaultWith (fun () -> c.EventStorePassword)
        member val Retries =                a.GetResult(Retries, 3)
        member val Timeout =                a.GetResult(Timeout, 20.) |> TimeSpan.FromSeconds
        member val Heartbeat =              a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
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
    and KafkaSinkArguments(c : Configuration, a : ParseResults<KafkaSinkParameters>) =
        member val Broker =                 a.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  a.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member val Producers =              a.GetResult(Producers, 1)
        member x.BuildTargetParams() =      x.Broker, x.Topic, x.Producers
#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

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
            member _.Meta = null
            member _.EventId = Guid.Empty
            member x.Timestamp = x.c
            member _.CorrelationId = null
            member _.CausationId = null
            member _.Context = null

    type Newtonsoft.Json.Linq.JObject with
        member document.Cast<'T>() =
            document.ToObject<'T>()

    /// We assume all Documents represent Events laid out as above
    let parse (d : Newtonsoft.Json.Linq.JObject) : Propulsion.Streams.StreamEvent<_> =
        let e = d.Cast<EventV0>()
        { stream = FsCodec.StreamName.parse e.s; event = e } : _

let transformV0 catFilter v0SchemaDocument : Propulsion.Streams.StreamEvent<_> seq = seq {
    let parsed = EventV0Parser.parse v0SchemaDocument
    let (FsCodec.StreamName.CategoryAndId (cat, _)) = parsed.stream
    if catFilter cat then
        yield parsed }
//#else
let transformOrFilter catFilter changeFeedDocument : Propulsion.Streams.StreamEvent<_> seq = seq {
    for { stream = FsCodec.StreamName.CategoryAndId (cat, _) } as e in Propulsion.CosmosStore.EquinoxNewtonsoftParser.enumStreamEvents changeFeedDocument do
        // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
        if catFilter cat then
            yield e }
//#endif

let [<Literal>] AppName = "SyncTemplate"

module Checkpoints =

    // In this implementation, we keep the checkpoints in Cosmos when consuming from EventStore
    module Cosmos =

        open Equinox.CosmosStore

        let codec = FsCodec.NewtonsoftJson.Codec.Create<Checkpoint.Events.Event>()
        let access = AccessStrategy.Custom (Checkpoint.Fold.isOrigin, Checkpoint.Fold.transmute)
        let create groupName (context, cache) =
            let caching = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
            let cat = CosmosStoreCategory(context, codec, Checkpoint.Fold.fold, Checkpoint.Fold.initial, caching, access)
            let resolve streamName = cat.Resolve(streamName, Equinox.AllowStale)
            Checkpoint.CheckpointSeries(groupName, resolve)

type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Sync.Stats<unit>(log, statsInterval, stateInterval)

    override _.HandleOk(()) = ()
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

let build (args : Args.Arguments, log) =
    let maybeDstCosmos, sink, streamFilter =
        match args.Sink with
        | Choice1Of2 cosmos ->
            let target = cosmos.Connect() |> Async.RunSynchronously
            let sink, streamFilter =
#if kafka
                let maxEvents, maxBytes = 100_000, 900_000
                match cosmos.KafkaSink with
                | Some kafka ->
                    let broker, topic, producers = kafka.BuildTargetParams()
                    let render (stream: FsCodec.StreamName, span: Propulsion.Streams.StreamSpan<_>) = async {
                        let value =
                            span
                            |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
                            |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
                        return FsCodec.StreamName.toString stream, value }
                    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, Confluent.Kafka.Acks.All, topic, degreeOfParallelism=producers)
                    let stats = Stats(Log.Logger, args.StatsInterval, args.StateInterval)
                    StreamsProducerSink.Start(
                        Log.Logger, args.MaxReadAhead, args.MaxWriters, render, producer, stats, args.StatsInterval, maxBytes=maxBytes, maxEvents=maxEvents),
                    args.CategoryFilterFunction(longOnly=true)
                | None ->
#endif
                let context = CosmosStoreContext.create target
                let eventsContext = Equinox.CosmosStore.Core.EventsContext(context, Config.log)
                Propulsion.CosmosStore.CosmosStoreSink.Start(log, args.MaxReadAhead, eventsContext, args.MaxWriters, args.StatsInterval, args.StateInterval, maxSubmissionsPerPartition=args.MaxSubmit),
                args.CategoryFilterFunction(excludeLong=true)
            Some target, sink, streamFilter
        | Choice2Of2 es ->
            let connect connIndex = async {
                let lfc = Config.log.ForContext("ConnId", connIndex)
                let! c = es.Connect(log, lfc, ConnectionStrategy.ClusterSingle NodePreference.Master, AppName, connIndex)
                return Context(c, BatchingPolicy(Int32.MaxValue)) }
            let targets = Array.init args.MaxConnections (string >> connect) |> Async.Parallel |> Async.RunSynchronously
            let sink = EventStoreSink.Start(log, Config.log, args.MaxReadAhead, targets, args.MaxWriters, args.StatsInterval, args.StateInterval, maxSubmissionsPerPartition=args.MaxSubmit)
            None, sink, args.CategoryFilterFunction()
    match args.SourceParams() with
    | Choice1Of2 (monitored, leases, processorName, startFromTail, maxItems, lagFrequency) ->
#if marveleqx
        use observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Seq.collect (transformV0 streamFilter))
#else
        use observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Seq.collect (transformOrFilter streamFilter))
#endif
        let runPipeline =
            Propulsion.CosmosStore.CosmosStoreSource.Run(
                Log.Logger, monitored, leases, processorName, observer, startFromTail,
                ?maxItems=maxItems, lagReportFreq=lagFrequency)
        sink, runPipeline
    | Choice2Of2 (srcE, checkpointsContext, spec) ->
        match maybeDstCosmos with
        | None -> failwith "ES->ES checkpointing E_NOTIMPL"
        | Some _ ->

        let cache = Equinox.Cache(AppName, sizeMb=1)
        let checkpoints = Checkpoints.Cosmos.create spec.groupName (checkpointsContext, cache)

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
    let log = Log.ForContext<Propulsion.Streams.Scheduling.StreamSchedulingEngine>()
    let sink, pipeline = build (args, log)
    pipeline |> Async.Start
    return! sink.AwaitWithStopOnCancellation()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(args.Verbose, args.StoreVerbose, ?maybeSeqEndpoint = args.MaybeSeqEndpoint).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
