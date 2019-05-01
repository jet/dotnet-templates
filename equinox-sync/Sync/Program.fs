module SyncTemplate.Program

open Equinox.Cosmos
//#if !eventStore
open Equinox.Cosmos.Projection
open Equinox.Cosmos.Projection.Ingestion
//#else
open Equinox.EventStore
//#endif
open Equinox.Projection.Engine
open Equinox.Projection.State
//#if !eventStore
open Equinox.Store
//#endif
open Serilog
open System
//#if !eventStore
open System.Collections.Generic
//#else
open System.Diagnostics
open System.Threading
//#endif

//#if eventStore
type StartPos = Absolute of int64 | Chunk of int | Percentage of float | TailOrCheckpoint | StartOrCheckpoint

type ReaderSpec =
    {   /// Identifier for this projection and it's state
        groupName: string
        /// Indicates user has specified that they wish to restart from the indicated position as opposed to resuming from the checkpoint position
        forceRestart: bool
        /// Start position from which forward reading is to commence // Assuming no stored position
        start: StartPos
        checkpointInterval: TimeSpan
        /// Delay when reading yields an empty batch
        tailInterval: TimeSpan
        gorge: bool
        /// Maximum number of striped readers to permit
        stripes: int
        /// Initial batch size to use when commencing reading
        batchSize: int
        /// Smallest batch size to degrade to in the presence of failures
        minBatchSize: int }
//#endif

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
        | [<AltCommandLine "-mi"; Unique>] BatchSize of int
        | [<AltCommandLine "-r"; Unique>] MaxPendingBatches of int
        | [<AltCommandLine "-mp"; Unique>] MaxProcessing of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
#if cosmos
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSource of string
        | [<AltCommandLine "-ad"; Unique>] LeaseCollectionDestination of string
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
#else
        | [<AltCommandLine "-vc"; Unique>] VerboseConsole
        | [<AltCommandLine "-f"; Unique>] ForceRestart
        | [<AltCommandLine "-mim"; Unique>] MinBatchSize of int
        | [<AltCommandLine "-p"; Unique>] Position of int64
        | [<AltCommandLine "-c"; Unique>] Chunk of int
        | [<AltCommandLine "-P"; Unique>] Percent of float
        | [<AltCommandLine "-g"; Unique>] Gorge
        | [<AltCommandLine "-i"; Unique>] Stripes of int
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
                | MaxProcessing _ ->        "Maximum number of batches to process concurrently. Default: 128"
                | MaxWriters _ ->           "Maximum number of concurrent writes to target permitted. Default: 512"
#if cosmos
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 1000"
                | LeaseCollectionSource _ ->"specify Collection Name for Leases collection, within `source` connection/database (default: `source`'s `collection` + `-aux`)."
                | LeaseCollectionDestination _ -> "specify Collection Name for Leases collection, within [destination] `cosmos` connection/database (default: defined relative to `source`'s `collection`)."
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | Source _ ->               "CosmosDb input parameters."
#else
                | VerboseConsole ->         "request Verbose Console Logging. Default: off"
                | FromTail ->               "Start the processing from the Tail"
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | ForceRestart _ ->         "Forget the current committed position; start from (and commit) specified position. Default: start from specified position or resume from committed."
                | MinBatchSize _ ->         "minimum item count to drop down to in reaction to read failures. Default: 512"
                | Position _ ->             "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"
                | Gorge ->                  "Parallel readers (instead of reading by stream)"
                | Stripes _ ->              "number of concurrent readers to run one chunk (256MB) apart. Default: 1"
                | Tail _ ->                 "attempt to read from tail at specified interval in Seconds. Default: 1"
                | Source _ ->               "EventStore input parameters."
#endif
    and Arguments(a : ParseResults<Parameters>) =
        member __.MaybeSeqEndpoint =    if a.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.Verbose =             a.Contains Verbose
        member __.MaxPendingBatches =   a.GetResult(MaxPendingBatches,5000)
        member __.MaxProcessing =       a.GetResult(MaxProcessing,128)
        member __.MaxWriters =          a.GetResult(MaxWriters,512)
#if cosmos
        member __.ChangeFeedVerbose =   a.Contains ChangeFeedVerbose
        member __.LeaseId =             a.GetResult ConsumerGroupName
        member __.BatchSize =           a.GetResult(BatchSize,1000)
        member __.StartFromHere =       a.Contains FromTail
        member __.LagFrequency =        a.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
#else
        member __.VerboseConsole =      a.Contains VerboseConsole
        member __.ConsumerGroupName =   a.GetResult ConsumerGroupName
        member __.ConsoleMinLevel =     if __.VerboseConsole then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
        member __.StartingBatchSize =   a.GetResult(BatchSize,4096)
        member __.MinBatchSize =        a.GetResult(MinBatchSize,512)
        member __.Gorge =               a.Contains(Gorge)
        member __.Stripes =             a.GetResult(Stripes,1)
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
            Log.Information("Processing Lease {leaseId} in Database {db} Collection {coll} in batches of {batchSize}", x.LeaseId, db.database, db.collection, x.BatchSize)
            if x.StartFromHere then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            disco, db, x.LeaseId, x.StartFromHere, x.BatchSize, x.LagFrequency
#else
        member x.BuildFeedParams() : ReaderSpec =
            let startPos =
                match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent, a.Contains FromTail with
                | Some p, _, _, _ ->   Absolute p
                | _, Some c, _, _ ->   StartPos.Chunk c
                | _, _, Some p, _ ->   Percentage p 
                | None, None, None, true -> StartPos.TailOrCheckpoint
                | None, None, None, _ -> StartPos.StartOrCheckpoint
            Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Collection {coll}",
                x.ConsumerGroupName, startPos, x.ForceRestart, x.Destination.Database, x.Destination.Collection)
            Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}] with {stripes} concurrent readers reading up to {maxPendingBatches} uncommitted batches ahead",
                x.MinBatchSize, x.StartingBatchSize, x.Stripes, x.MaxPendingBatches)
            Log.Information("Max batches to process concurrently: {maxProcessing}",
                x.MaxProcessing)
            {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = x.CheckpointInterval; tailInterval = x.TailInterval; forceRestart = x.ForceRestart
                batchSize = x.StartingBatchSize; minBatchSize = x.MinBatchSize; gorge = x.Gorge; stripes = x.Stripes }
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

#if !cosmos 
module EventStoreSource =
    type TailAndPrefixesReader(conn, batchSize, minBatchSize, tryMapEvent: EventStore.ClientAPI.ResolvedEvent -> StreamItem option, maxDop, ?statsInterval) = 
        let sleepIntervalMs = 100
        let dop = new SemaphoreSlim(maxDop)
        let work = EventStoreSource.ReadQueue(batchSize, minBatchSize, ?statsInterval=statsInterval)
        member __.HasCapacity = work.QueueCount < dop.CurrentCount
        // TODO stuff
        member __.AddStreamPrefix(stream, pos, len) = work.AddStreamPrefix(stream, pos, len)
        member __.Pump(post, startPos, max, tailInterval) = async {
            let! ct = Async.CancellationToken
            work.AddTail(startPos, max, tailInterval)
            while not ct.IsCancellationRequested do
                work.OverallStats.DumpIfIntervalExpired()
                let! _ = dop.Await()
                let forkRunRelease task = async {
                    let! _ = Async.StartChild <| async {
                        try let! _ = work.Process(conn, tryMapEvent, post, task) in ()
                        finally dop.Release() |> ignore }
                    return () }
                match work.TryDequeue() with
                | true, task ->
                    do! forkRunRelease task
                | false, _ ->
                    dop.Release() |> ignore
                    do! Async.Sleep sleepIntervalMs } 

    type StripedReader(conn, batchSize, minBatchSize, tryMapEvent: EventStore.ClientAPI.ResolvedEvent -> StreamItem option, maxDop, ?statsInterval) = 
        let dop = new SemaphoreSlim(maxDop)
        let work = EventStoreSource.ReadQueue(batchSize, minBatchSize, ?statsInterval=statsInterval)

        member __.Pump(post, startPos, max) = async {
            let! ct = Async.CancellationToken
            let mutable remainder =
                let nextPos = EventStoreSource.posFromChunkAfter startPos
                work.AddTranche(startPos, nextPos, max)
                Some nextPos
            let mutable finished = false
            while not ct.IsCancellationRequested && not (finished && dop.CurrentCount <> maxDop) do
                work.OverallStats.DumpIfIntervalExpired()
                let! _ = dop.Await()
                let forkRunRelease task = async {
                    let! _ = Async.StartChild <| async {
                        try let! eof = work.Process(conn, tryMapEvent, post, task) in ()
                            if eof then remainder <- None
                        finally dop.Release() |> ignore }
                    return () }
                match remainder with
                | Some pos -> 
                    let nextPos = EventStoreSource.posFromChunkAfter pos
                    remainder <- Some nextPos
                    do! forkRunRelease <| EventStoreSource.Work.Tranche (EventStoreSource.Range(pos, Some nextPos, max), batchSize)
                | None ->
                    if finished then do! Async.Sleep 1000 
                    else Log.Error("No further ingestion work to commence")
                    finished <- true }

    type StartMode = Starting | Resuming | Overridding
            
    //// 4. Enqueue streams with gaps if there is capacity (not overloading, to avoid redundant work)
    //let mutable more = true 
    //while more && readers.HasCapacity do
    //    match buffer.TryGap() with
    //    | Some (stream,pos,len) -> readers.AddStreamPrefix(stream,pos,len)
    //    | None -> more <- false

    let run (log : Serilog.ILogger) (conn, spec, tryMapEvent) maxReadAhead maxProcessing (cosmosContext, maxWriters) resolveCheckpointStream = async {
        let checkpoints = Checkpoint.CheckpointSeries(spec.groupName, log.ForContext<Checkpoint.CheckpointSeries>(), resolveCheckpointStream)
        let! maxInParallel = Async.StartChild <| EventStoreSource.establishMax conn 
        let! initialCheckpointState = checkpoints.Read
        let! max = maxInParallel
        let! startPos = async {
            let mkPos x = EventStore.ClientAPI.Position(x, 0L)
            let requestedStartPos =
                match spec.start with
                | Absolute p -> mkPos p
                | Chunk c -> EventStoreSource.posFromChunk c
                | Percentage pct -> EventStoreSource.posFromPercentage (pct, max)
                | TailOrCheckpoint -> max
                | StartOrCheckpoint -> EventStore.ClientAPI.Position.Start
            let! startMode, startPos, checkpointFreq = async {
                match initialCheckpointState, requestedStartPos with
                | Checkpoint.Folds.NotStarted, r ->
                    if spec.forceRestart then raise <| CmdParser.InvalidArguments ("Cannot specify --forceRestart when no progress yet committed")
                    do! checkpoints.Start(spec.checkpointInterval, r.CommitPosition)
                    return Starting, r, spec.checkpointInterval
                | Checkpoint.Folds.Running s, _ when not spec.forceRestart ->
                    return Resuming, mkPos s.state.pos, TimeSpan.FromSeconds(float s.config.checkpointFreqS)
                | Checkpoint.Folds.Running _, r ->
                    do! checkpoints.Override(spec.checkpointInterval, r.CommitPosition)
                    return Overridding, r, spec.checkpointInterval
            }
            log.Information("Sync {mode} {groupName} @ {pos} (chunk {chunk}, {pct:p1}) tailing every {interval}s, checkpointing every {checkpointFreq}m",
                startMode, spec.groupName, startPos.CommitPosition, EventStoreSource.chunk startPos,
                float startPos.CommitPosition/float max.CommitPosition, spec.tailInterval.TotalSeconds, checkpointFreq.TotalMinutes)
            return startPos }
        let ingestionEngine = startIngestionEngine (log, maxProcessing, cosmosContext, maxWriters, TimeSpan.FromMinutes 1.)
        let trancheEngine = TrancheEngine.Start (log, ingestionEngine, maxReadAhead, maxProcessing, TimeSpan.FromMinutes 1.)
        if spec.gorge then
            let readers = StripedReader(conn, spec.batchSize, spec.minBatchSize, tryMapEvent, spec.stripes + 1)
            let post = function
                | EventStoreSource.ReadItem.Batch (pos, xs) ->
                    let cp = pos.CommitPosition
                    let chunk = EventStoreSource.chunk pos
                    trancheEngine.Submit <| Push.ChunkBatch(int chunk, cp, checkpoints.Commit cp, xs)
                | EventStoreSource.ReadItem.EndOfTranche chunk ->
                    trancheEngine.Submit <| Push.EndOfChunk chunk
                | EventStoreSource.ReadItem.StreamSpan _ as x ->
                    failwithf "%A not supported when gorging" x
            let startChunk = EventStoreSource.chunk startPos |> int
            let! _ = trancheEngine.Submit (Push.SetActiveChunk startChunk)
            do! readers.Pump(post, startPos, max)
        else
            let post = function
                | EventStoreSource.ReadItem.Batch (pos, xs) ->
                    let cp = pos.CommitPosition
                    trancheEngine.Submit(cp, checkpoints.Commit cp, xs)
                | EventStoreSource.ReadItem.StreamSpan span ->
                    trancheEngine.Submit <| Push.Stream span
                | EventStoreSource.ReadItem.EndOfTranche _ as x ->
                    failwithf "%A not supported" x
            let readers = TailAndPrefixesReader(conn, spec.batchSize, spec.minBatchSize, tryMapEvent, spec.stripes + 1)
            do! readers.Pump(post, startPos, max, spec.tailInterval) }
#else
module CosmosSource =
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing

    let createRangeSyncHandler (log:ILogger) maxPendingBatches (cosmosContext: Core.CosmosContext, maxWriters) (transform : Microsoft.Azure.Documents.Document -> StreamItem seq) =
        let ingestionEngine = Equinox.Cosmos.Projection.Ingestion.startIngestionEngine (log, maxPendingBatches, cosmosContext, maxWriters, TimeSpan.FromMinutes 1.)
        let maxUnconfirmedBatches = 10
        let mutable trancheEngine = Unchecked.defaultof<_>
        let init rangeLog =
            trancheEngine <- Equinox.Projection.Engine.TrancheEngine.Start (rangeLog, ingestionEngine, maxPendingBatches, maxWriters, TimeSpan.FromMinutes 1.)
        let ingest epoch checkpoint docs =
            let events = docs |> Seq.collect transform |> Array.ofSeq
            trancheEngine.Submit(epoch, checkpoint, events)
        let dispose () = trancheEngine.Stop ()
        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let processBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let epoch = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64
            // Pass along the function that the coordinator will run to checkpoint past this batch when such progress has been achieved
            let checkpoint = async { do! ctx.CheckpointAsync() |> Async.AwaitTaskCorrect }
            let! pt, (cur,max) = ingest epoch checkpoint docs |> Stopwatch.Time
            log.Information("Read -{token,6} {count,4} docs {requestCharge,6}RU {l:n1}s Post {pt:n3}s {cur}/{max}",
                epoch, docs.Count, (let c = ctx.FeedResponse.RequestCharge in c.ToString("n1")), float sw.ElapsedMilliseconds / 1000.,
                let e = pt.Elapsed in e.TotalSeconds, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, processBatch, assign=init, dispose=dispose)

    let run (sourceDiscovery, source) (auxDiscovery, aux) connectionPolicy (leaseId, forceSkip, batchSize, lagReportFreq : TimeSpan option)
            createRangeProjector = async {
        let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
            Log.Information("Lags {@rangeLags} (Range, Docs count)", remainingWork)
            return! Async.Sleep interval }
        let maybeLogLag = lagReportFreq |> Option.map logLag
        let! _feedEventHost =
            ChangeFeedProcessor.Start
              ( Log.Logger, sourceDiscovery, connectionPolicy, source, aux, auxDiscovery = auxDiscovery, leasePrefix = leaseId, forceSkipExistingEvents = forceSkip,
                cfBatchSize = batchSize, createObserver = createRangeProjector, ?reportLagAndAwaitNextEstimation = maybeLogLag)
        do! Async.AwaitKeyboardInterrupt() }

    //#if marveleqx
    [<RequireQualifiedAccess>]
    module EventV0Parser =
        open Newtonsoft.Json

        /// A single Domain Event as Written by internal Equinox versions
        type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
            EventV0 =
            {   /// DocDb-mandated Partition Key, must be maintained within the document
                s: string // "{streamName}"

                /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
                c: DateTimeOffset // ISO 8601

                /// The Case (Event Type); used to drive deserialization
                t: string // required

                /// 'i' value for the Event
                i: int64 // {index}

                /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
                [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
                d: byte[] }

        type Document with
            member document.Cast<'T>() =
                let tmp = new Document()
                tmp.SetPropertyValue("content", document)
                tmp.GetPropertyValue<'T>("content")
        type IEvent =
            inherit Equinox.Codec.Core.IIndexedEvent<byte[]>
                abstract member Stream : string
        /// We assume all Documents represent Events laid out as above
        let parse (d : Document) =
            let x = d.Cast<EventV0>()
            { new IEvent with
                  member __.Index = x.i
                  member __.IsUnfold = false
                  member __.EventType = x.t
                  member __.Data = x.d
                  member __.Meta = null
                  member __.Timestamp = x.c
                  member __.Stream = x.s }

    let transformV0 catFilter (v0SchemaDocument: Document) : StreamItem seq = seq {
        let parsed = EventV0Parser.parse v0SchemaDocument
        let streamName = (*if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+*)parsed.Stream
        if catFilter (category streamName) then
            yield { stream = streamName; index = parsed.Index; event = parsed } }
    //#else
    let transformOrFilter catFilter (changeFeedDocument: Document) : StreamItem seq = seq {
        for e in DocumentParser.enumEvents changeFeedDocument do
            if catFilter (category e.Stream) then
                // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
                yield { stream = e.Stream; index = e.Index; event =  e } }
    //#endif
#endif

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
                        c.MinimumLevel.Override(typeof<StreamStates>.FullName, ingesterLevel)
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<Writer.Result>.FullName, generalLevel)
                         .MinimumLevel.Override(typeof<Checkpoint.CheckpointSeries>.FullName, LogEventLevel.Information)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                            a.Logger(fun l ->
                                l.WriteTo.Sink(Metrics.RuCounters.RuCounterSink()) |> ignore) |> ignore
                            a.Logger(fun l ->
                                let isEqx = Filters.Matching.FromSource<Core.CosmosContext>().Invoke
                                let isWriter = Filters.Matching.FromSource<Writer.Result>().Invoke
                                let isCheckpointing = Filters.Matching.FromSource<Checkpoint.CheckpointSeries>().Invoke
                                (if verboseConsole then l else l.Filter.ByExcluding(fun x -> isEqx x || isCheckpointing x || isWriter x))
                                    .WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                                    |> ignore) |> ignore
                        c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=Action<_> configure)
            |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            |> fun c -> c.CreateLogger()
        Log.ForContext<StreamStates>(), Log.ForContext<Core.CosmosContext>()

[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
#if cosmos
        let log,storeLog = Logging.initialize args.Verbose args.ChangeFeedVerbose args.MaybeSeqEndpoint
#else
        let log,storeLog = Logging.initialize args.Verbose args.VerboseConsole args.MaybeSeqEndpoint
#endif
        let destination = args.Destination.Connect "SyncTemplate" |> Async.RunSynchronously
        let colls = CosmosCollections(args.Destination.Database, args.Destination.Collection)
        let resolveCheckpointStream =
            let gateway = CosmosGateway(destination, CosmosBatchingPolicy())
            let store = Equinox.Cosmos.CosmosStore(gateway, colls)
            let settings = Newtonsoft.Json.JsonSerializerSettings()
            let codec = Equinox.Codec.NewtonsoftJson.Json.Create settings
            let caching =
                let c = Equinox.Cosmos.Caching.Cache("SyncTemplate", sizeMb = 1)
                Equinox.Cosmos.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            let access = Equinox.Cosmos.AccessStrategy.Snapshot (Checkpoint.Folds.isOrigin, Checkpoint.Folds.unfold)
            Equinox.Cosmos.CosmosResolver(store, codec, Checkpoint.Folds.fold, Checkpoint.Folds.initial, caching, access).Resolve
        let target = Equinox.Cosmos.Core.CosmosContext(destination, colls, storeLog)
#if cosmos
        let discovery, source, connectionPolicy, catFilter = args.Source.BuildConnectionDetails()
        let auxDiscovery, aux, leaseId, startFromHere, batchSize, lagFrequency = args.BuildChangeFeedParams()
#if marveleqx
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log args.MaxPendingBatches (target, args.MaxWriters) (CosmosSource.transformV0 catFilter)
#else
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log args.MaxPendingBatches (target, args.MaxWriters) (CosmosSource.transformOrFilter catFilter)
        // Uncomment to test marveleqx mode
        // let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#endif
        CosmosSource.run (discovery, source) (auxDiscovery, aux) connectionPolicy
            (leaseId, startFromHere, batchSize, lagFrequency)
            createSyncHandler
#else
        let esConnection = args.Source.Connect(log, log, ConnectionStrategy.ClusterSingle NodePreference.PreferSlave)
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
                || e.EventStreamId.StartsWith("InventoryLog") // 5GB, causes lopsided partitions, unused
                || e.EventStreamId = "ReloadBatchId" // does not start at 0
                || e.EventStreamId = "PurchaseOrder-5791" // item too large
                || e.EventStreamId = "SkuFileUpload-99682b9cdbba4b09881d1d87dfdc1ded"
                || e.EventStreamId = "SkuFileUpload-1e0626cc418548bc8eb82808426430e2"
                || e.EventStreamId = "SkuFileUpload-6b4f566d90194263a2700c0ad1bc54dd"
                || e.EventStreamId = "SkuFileUpload-5926b2d7512c4f859540f7f20e35242b"
                || e.EventStreamId = "SkuFileUpload-9ac536b61fed4b44853a1f5e2c127d50"
                || e.EventStreamId = "SkuFileUpload-b501837ce7e6416db80ca0c48a4b3f7a"
                || e.EventStreamId = "Inventory-FC000" // Too long
                || not (catFilter e.EventStreamId) -> None
            | e -> e |> EventStoreSource.toIngestionItem |> Some
        EventStoreSource.run log (esConnection.ReadConnection, spec, tryMapEvent catFilter) args.MaxPendingBatches args.MaxProcessing (target, args.MaxWriters) resolveCheckpointStream
#endif
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.InvalidArguments msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1