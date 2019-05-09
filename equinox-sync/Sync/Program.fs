module SyncTemplate.Program

open Equinox.Cosmos
#if cosmos
open Equinox.Cosmos.Projection
#else
open Equinox.EventStore
#endif
open Serilog
open System

module CmdParser =
    open Argu

    exception InvalidArguments of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| InvalidArguments (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine "-S"; Unique>] LocalSeq
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-z"; Unique>] FromTail
        | [<AltCommandLine "-r"; Unique>] MaxPendingBatches of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
        | [<AltCommandLine "-cc"; Unique>] MaxCosmosConnections of int
#if cosmos
        | [<AltCommandLine "-mp"; Unique>] MaxProcessing of int
        | [<AltCommandLine "-md"; Unique>] MaxDocuments of int
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSource of string
        | [<AltCommandLine "-ad"; Unique>] LeaseCollectionDestination of string
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
#else
        | [<AltCommandLine "-vc"; Unique>] VerboseConsole
        | [<AltCommandLine "-f"; Unique>] ForceRestart
        | [<AltCommandLine "-mi"; Unique>] BatchSize of int
        | [<AltCommandLine "-mim"; Unique>] MinBatchSize of int
        | [<AltCommandLine "-p"; Unique>] Position of int64
        | [<AltCommandLine "-c"; Unique>] Chunk of int
        | [<AltCommandLine "-P"; Unique>] Percent of float
        | [<AltCommandLine "-g"; Unique>] Gorge of int
        | [<AltCommandLine "-i"; Unique>] StreamReaders of int
        | [<AltCommandLine "-t"; Unique>] Tail of intervalS: float
#endif
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Source of ParseResults<SourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | Verbose ->                "request Verbose Logging. Default: off"
                | MaxPendingBatches _ ->    "Maximum number of batches to let processing get ahead of completion. Default: 2048"
                | MaxWriters _ ->           "Maximum number of concurrent writes to target permitted. Default: 512"
                | MaxCosmosConnections _ -> "Size of CosmosDb connection pool to maintain. Default: 1"
#if cosmos
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited"
                | MaxProcessing _ ->        "Maximum number of batches to submit concurrently. Default: 16"
                | LeaseCollectionSource _ ->"specify Collection Name for Leases collection, within `source` connection/database (default: `source`'s `collection` + `-aux`)."
                | LeaseCollectionDestination _ -> "specify Collection Name for Leases collection, within [destination] `cosmos` connection/database (default: defined relative to `source`'s `collection`)."
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | Source _ ->               "CosmosDb input parameters."
#else
                | VerboseConsole ->         "request Verbose Console Logging. Default: off"
                | FromTail ->               "Start the processing from the Tail"
                | ForceRestart _ ->         "Forget the current committed position; start from (and commit) specified position. Default: start from specified position or resume from committed."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | MinBatchSize _ ->         "minimum item count to drop down to in reaction to read failures. Default: 512"
                | Position _ ->             "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"
                | Gorge _ ->                 "Request Parallel readers phase during initial catchup, running one chunk (256MB) apart. Default: off"
                | StreamReaders _ ->        "number of concurrent readers that will fetch a missing stream when in tailing mode. Default: 1. TODO: IMPLEMENT!"
                | Tail _ ->                 "attempt to read from tail at specified interval in Seconds. Default: 1"
                | Source _ ->               "EventStore input parameters."
#endif
    and Arguments(a : ParseResults<Parameters>) =
        member __.MaybeSeqEndpoint =    if a.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.Verbose =             a.Contains Verbose
        member __.MaxPendingBatches =   a.GetResult(MaxPendingBatches,2048)
        member __.MaxWriters =          a.GetResult(MaxWriters,1024)
        member __.CosmosConnectionPool =a.GetResult(MaxCosmosConnections,1)
#if cosmos
        member __.ChangeFeedVerbose =   a.Contains ChangeFeedVerbose
        member __.LeaseId =             a.GetResult ConsumerGroupName
        member __.MaxDocuments =        a.TryGetResult MaxDocuments
        member __.MaxProcessing =       a.GetResult(MaxProcessing,16)
        member __.LagFrequency =        a.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
#else
        member __.VerboseConsole =      a.Contains VerboseConsole
        member __.ConsumerGroupName =   a.GetResult ConsumerGroupName
        member __.ConsoleMinLevel =     if __.VerboseConsole then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
        member __.StartingBatchSize =   a.GetResult(BatchSize,4096)
        member __.MinBatchSize =        a.GetResult(MinBatchSize,512)
        member __.Gorge =               a.TryGetResult Gorge
        member __.StreamReaders =       a.GetResult(StreamReaders,1)
        member __.TailInterval =        a.GetResult(Tail,1.) |> TimeSpan.FromSeconds
        member __.CheckpointInterval =  TimeSpan.FromHours 1.
        member __.ForceRestart =        a.Contains ForceRestart
#endif

        member val Source : SourceArguments = SourceArguments(a.GetResult Source)
        member __.Destination : DestinationArguments = __.Source.Destination
#if cosmos
        member x.BuildChangeFeedParams() =
            let disco, db =
                match a.TryGetResult LeaseCollectionSource, a.TryGetResult LeaseCollectionDestination with
                | None, None ->     x.Source.Discovery, { database = x.Source.Database; collection = x.Source.Collection + "-aux" }
                | Some sc, None ->  x.Source.Discovery, { database = x.Source.Database; collection = sc }
                | None, Some dc ->  x.Destination.Discovery, { database = x.Destination.Database; collection = dc }
                | Some _, Some _ -> raise (InvalidArguments "LeaseCollectionSource and LeaseCollectionDestination are mutually exclusive - can only store in one database")
            Log.Information("Max batches to process concurrently per Range: {maxProcessing}", x.MaxProcessing)
            Log.Information("Processing Lease {leaseId} in Database {db} Collection {coll} with maximum document count limited to {maxDocuments}", x.LeaseId, db.database, db.collection, x.MaxDocuments)
            if a.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            disco, db, x.LeaseId, a.Contains FromTail, x.MaxDocuments, x.LagFrequency
#else
        member x.BuildFeedParams() : EventStoreSource.ReaderSpec =
            let startPos =
                match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent, a.Contains FromTail with
                | Some p, _, _, _ ->   EventStoreSource.Absolute p
                | _, Some c, _, _ ->   EventStoreSource.StartPos.Chunk c
                | _, _, Some p, _ ->   EventStoreSource.Percentage p 
                | None, None, None, true -> EventStoreSource.StartPos.TailOrCheckpoint
                | None, None, None, _ -> EventStoreSource.StartPos.StartOrCheckpoint
            Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Collection {coll}",
                x.ConsumerGroupName, startPos, x.ForceRestart, x.Destination.Database, x.Destination.Collection)
            Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}], reading up to {maxPendingBatches} uncommitted batches ahead",
                x.MinBatchSize, x.StartingBatchSize, x.MaxPendingBatches)
            {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = x.CheckpointInterval; tailInterval = x.TailInterval; forceRestart = x.ForceRestart
                batchSize = x.StartingBatchSize; minBatchSize = x.MinBatchSize; gorge = x.Gorge; streamReaders = x.StreamReaders }
#endif
    and [<NoEquality; NoComparison>] SourceParameters =
#if cosmos
        | [<AltCommandLine "-m">] SourceConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">] SourceTimeout of float
        | [<AltCommandLine "-r">] SourceRetries of int
        | [<AltCommandLine "-rt">] SourceRetriesWaitTime of int
        | [<AltCommandLine "-s">] SourceConnection of string
        | [<AltCommandLine "-d">] SourceDatabase of string
        | [<AltCommandLine "-c"; Unique(*Mandatory is not supported*)>] SourceCollection of string
#else
        | [<AltCommandLine("-v")>] VerboseStore
        | [<AltCommandLine("-o")>] SourceTimeout of float
        | [<AltCommandLine("-r")>] SourceRetries of int
        | [<AltCommandLine("-g")>] Host of string
        | [<AltCommandLine("-x")>] Port of int
        | [<AltCommandLine("-u")>] Username of string
        | [<AltCommandLine("-p")>] Password of string
        | [<AltCommandLine("-h")>] HeartbeatTimeout of float
#endif
        | [<AltCommandLine "-e">] CategoryBlacklist of string
        | [<AltCommandLine "-i">] CategoryWhitelist of string
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<DestinationParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
#if cosmos
                | SourceConnection _ ->     "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION)."
                | SourceDatabase _ ->       "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE)."
                | SourceCollection _ ->     "specify a collection name within `SourceDatabase`."
                | SourceTimeout _ ->        "specify operation timeout in seconds (default: 5)."
                | SourceRetries _ ->        "specify operation retries (default: 1)."
                | SourceRetriesWaitTime _ ->"specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | SourceConnectionMode _ -> "override the connection mode (default: DirectTcp)."
#else
                | VerboseStore ->           "Include low level Store logging."
                | SourceTimeout _ ->        "specify operation timeout in seconds (default: 20)."
                | SourceRetries _ ->        "specify operation retries (default: 3)."
                | Host _ ->                 "specify a DNS query, using Gossip-driven discovery against all A records returned (defaults: envvar:EQUINOX_ES_HOST, localhost)."
                | Port _ ->                 "specify a custom port (default: envvar:EQUINOX_ES_PORT, 30778)."
                | Username _ ->             "specify a username (defaults: envvar:EQUINOX_ES_USERNAME, admin)."
                | Password _ ->             "specify a Password (defaults: envvar:EQUINOX_ES_PASSWORD, changeit)."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds (default: 1.5)."
#endif
                | CategoryBlacklist _ ->    "Category whitelist"
                | CategoryWhitelist _ ->    "Category blacklist"
                | Cosmos _ ->               "CosmosDb destination parameters."
    and SourceArguments(a : ParseResults<SourceParameters>) =
        member val Destination =        DestinationArguments(a.GetResult Cosmos)
        member __.CategoryFilterFunction : string -> bool =
            match a.GetResults CategoryBlacklist, a.GetResults CategoryWhitelist with
            | [], [] ->     Log.Information("Not filtering by category"); fun _ -> true 
            | bad, [] ->    let black = Set.ofList bad in Log.Warning("Excluding categories: {cats}", black); fun x -> not (black.Contains x)
            | [], good ->   let white = Set.ofList good in Log.Warning("Only copying categories: {cats}", white); fun x -> white.Contains x
            | _, _ -> raise (InvalidArguments "BlackList and Whitelist are mutually exclusive; inclusions and exclusions cannot be mixed")
#if cosmos
        member __.Mode =                a.GetResult(SourceConnectionMode, Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Discovery =           Discovery.FromConnectionString __.Connection
        member __.Connection =          match a.TryGetResult SourceConnection   with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult SourceDatabase     with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          a.GetResult SourceCollection

        member __.Timeout =             a.GetResult(SourceTimeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(SourceRetries, 1)
        member __.MaxRetryWaitTime =    a.GetResult(SourceRetriesWaitTime, 5)
        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri,_)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Collection {collection}",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; collection = x.Collection }, c.ConnectionPolicy, x.CategoryFilterFunction
#else
        member __.Host =                match a.TryGetResult Host       with Some x -> x | None -> envBackstop "Host"       "EQUINOX_ES_HOST"
        member __.Port =                match a.TryGetResult Port       with Some x -> Some x | None -> Environment.GetEnvironmentVariable "EQUINOX_ES_PORT" |> Option.ofObj |> Option.map int
        member __.Discovery =           match __.Port                   with Some p -> Discovery.GossipDnsCustomPort (__.Host, p) | None -> Discovery.GossipDns __.Host 
        member __.User =                match a.TryGetResult Username   with Some x -> x | None -> envBackstop "Username"   "EQUINOX_ES_USERNAME"
        member __.Password =            match a.TryGetResult Password   with Some x -> x | None -> envBackstop "Password"   "EQUINOX_ES_PASSWORD"
        member __.Heartbeat =           a.GetResult(HeartbeatTimeout,1.5) |> TimeSpan.FromSeconds
        member __.Timeout =             a.GetResult(SourceTimeout,20.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(SourceRetries,3)
        member __.Connect(log: ILogger, storeLog: ILogger, connectionStrategy) =
            let s (x : TimeSpan) = x.TotalSeconds
            log.Information("EventStore {host} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}", __.Host, s __.Heartbeat, s __.Timeout, __.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            GesConnector(__.User, __.Password, __.Timeout, __.Retries, log=log, heartbeatTimeout=__.Heartbeat, tags=tags)
                .Establish("SyncTemplate", __.Discovery, connectionStrategy) |> Async.RunSynchronously
#endif
    and [<NoEquality; NoComparison>] DestinationParameters =
        | [<AltCommandLine("-s")>] Connection of string
        | [<AltCommandLine("-d")>] Database of string
        | [<AltCommandLine("-c")>] Collection of string
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-rt")>] RetriesWaitTime of int
        | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Connection _ ->           "specify a connection string for a Cosmos account (default: envvar:EQUINOX_COSMOS_CONNECTION)."
                | Database _ ->             "specify a database name for Cosmos account (default: envvar:EQUINOX_COSMOS_DATABASE)."
                | Collection _ ->           "specify a collection name for Cosmos account (default: envvar:EQUINOX_COSMOS_COLLECTION)."
                | Timeout _ ->              "specify operation timeout in seconds (default: 5)."
                | Retries _ ->              "specify operation retries (default: 0)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->       "override the connection mode (default: DirectTcp)."
    and DestinationArguments(a : ParseResults<DestinationParameters>) =
        member __.Mode =                a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Discovery =           Discovery.FromConnectionString __.Connection
        member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

        member __.Timeout =             a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries, 0)
        member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5)

        /// Connect with the provided parameters and/or environment variables
        member x.Connect
            /// Connection/Client identifier for logging purposes
            name : Async<CosmosConnection> =
            let (Discovery.UriAndKey (endpointUri,_masterKey)) as discovery = x.Discovery
            Log.Information("Destination CosmosDb {mode} {endpointUri} Database {database} Collection {collection}",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("Destination CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c.Connect(name, discovery)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    open Serilog.Events
    let initialize verbose verboseConsole maybeSeqEndpoint =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
                        let cfpLevel = if verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpLevel)
            |> fun c -> let ingesterLevel = if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information
                        c.MinimumLevel.Override(typeof<Equinox.Projection.State.StreamStates>.FullName, ingesterLevel)
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<Equinox.Cosmos.Projection.CosmosIngester.Writer.Result>.FullName, generalLevel)
                         .MinimumLevel.Override(typeof<Checkpoint.CheckpointSeries>.FullName, LogEventLevel.Information)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                            a.Logger(fun l ->
                                l.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.RuCounters.RuCounterSink()) |> ignore) |> ignore
                            a.Logger(fun l ->
                                let isEqx = Filters.Matching.FromSource<Core.CosmosContext>().Invoke
                                let isCp = Filters.Matching.FromSource<Checkpoint.CheckpointSeries>().Invoke
                                let isWriter = Filters.Matching.FromSource<Equinox.Cosmos.Projection.CosmosIngester.Writer.Result>().Invoke
                                let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                                let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                                let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                                (if verboseConsole then l else l.Filter.ByExcluding(fun x -> isEqx x || isCp x || isWriter x || isCfp429a x || isCfp429b x || isCfp429c x))
                                    .WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                                    |> ignore) |> ignore
                        c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=Action<_> configure)
            |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            |> fun c -> c.CreateLogger()
        Log.ForContext<Equinox.Projection.State.StreamStates>(), Log.ForContext<Core.CosmosContext>()

[<EntryPoint>]
let main argv =
    try //if not (System.Threading.ThreadPool.SetMaxThreads(512,512)) then raise (CmdParser.InvalidArguments "Could not set thread limits")
        let args = CmdParser.parse argv
#if cosmos
        let log,storeLog = Logging.initialize args.Verbose args.ChangeFeedVerbose args.MaybeSeqEndpoint
#else
        let log,storeLog = Logging.initialize args.Verbose args.VerboseConsole args.MaybeSeqEndpoint
#endif
        let destinations = Seq.init args.CosmosConnectionPool (fun i -> args.Destination.Connect (sprintf "%s Pool %d" "SyncTemplate" i)) |> Async.Parallel |> Async.RunSynchronously
        let colls = CosmosCollections(args.Destination.Database, args.Destination.Collection)
        let resolveCheckpointStream =
            let gateway = CosmosGateway(destinations.[0], CosmosBatchingPolicy())
            let store = Equinox.Cosmos.CosmosStore(gateway, colls)
            let settings = Newtonsoft.Json.JsonSerializerSettings()
            let codec = Equinox.Codec.NewtonsoftJson.Json.Create settings
            let caching =
                let c = Equinox.Cosmos.Caching.Cache("SyncTemplate", sizeMb = 1)
                Equinox.Cosmos.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            let access = Equinox.Cosmos.AccessStrategy.Snapshot (Checkpoint.Folds.isOrigin, Checkpoint.Folds.unfold)
            Equinox.Cosmos.CosmosResolver(store, codec, Checkpoint.Folds.fold, Checkpoint.Folds.initial, caching, access).Resolve
        let targets = destinations |> Array.mapi (fun i x -> Equinox.Cosmos.Core.CosmosContext(x, colls, storeLog.ForContext("PoolId", i)))
#if cosmos
        let discovery, source, connectionPolicy, catFilter = args.Source.BuildConnectionDetails()
        let auxDiscovery, aux, leaseId, startFromHere, maxDocuments, lagFrequency = args.BuildChangeFeedParams()
#if marveleqx
        let createSyncHandler = CosmosSource.createRangeSyncHandler log (CosmosSource.transformV0 catFilter)
#else
        let createSyncHandler = CosmosSource.createRangeSyncHandler log (CosmosSource.transformOrFilter catFilter)
        // Uncomment to test marveleqx mode
        // let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#endif
        CosmosSource.run log (discovery, source) (auxDiscovery, aux) connectionPolicy
            (leaseId, startFromHere, maxDocuments, lagFrequency)
            (targets, args.MaxWriters)
            (createSyncHandler (args.MaxPendingBatches*2,args.MaxProcessing))
#else
        let connect () = let c = args.Source.Connect(log, log, ConnectionStrategy.ClusterSingle NodePreference.PreferSlave) in c.ReadConnection 
        let catFilter = args.Source.CategoryFilterFunction
        let spec = args.BuildFeedParams()
        let tryMapEvent catFilter (x : EventStore.ClientAPI.ResolvedEvent) =
            match x.Event with
            | e when not e.IsJson
                || e.EventStreamId.StartsWith("$") 
                || e.EventType.StartsWith("compacted",StringComparison.OrdinalIgnoreCase)
                || e.EventStreamId.StartsWith "marvel_bookmark"
                || e.EventStreamId.EndsWith "_checkpoints"
                || e.EventStreamId.EndsWith "_checkpoint"
                || e.EventStreamId.StartsWith "Inventory-" // Too long
                || e.EventStreamId.StartsWith "InventoryCount-" // No Longer used
                || e.EventStreamId.StartsWith "InventoryLog" // 5GB, causes lopsided partitions, unused
                || e.EventStreamId = "SkuFileUpload-534e4362c641461ca27e3d23547f0852"
                || e.EventStreamId = "SkuFileUpload-778f1efeab214f5bab2860d1f802ef24"
                || e.EventStreamId = "PurchaseOrder-5791" // item too large
                || not (catFilter e.EventStreamId) -> None
            | e -> e |> EventStoreSource.toIngestionItem |> Some
        EventStoreSource.run log (connect, spec, tryMapEvent catFilter) args.MaxPendingBatches (targets, args.MaxWriters) resolveCheckpointStream
#endif
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.InvalidArguments msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1