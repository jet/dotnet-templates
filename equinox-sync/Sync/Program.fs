module SyncTemplate.Program

open Equinox.Cosmos
open Equinox.Cosmos.Core
//#if !eventStore
open Equinox.Cosmos.Projection
//#endif
//#if eventStore
open Equinox.EventStore
//#endif
//#if !eventStore
open Equinox.Store
//#endif
open Serilog
open System
//#if !eventStore
open System.Collections.Generic
//#endif
open System.Diagnostics
open System.Threading

let mb x = float x / 1024. / 1024.
let category (streamName : string) = streamName.Split([|'-'|],2).[0]
let every ms f =
    let timer = Stopwatch.StartNew()
    fun () ->
        if timer.ElapsedMilliseconds > ms then
            f ()
            timer.Restart()

//#if eventStore
type StartPos = Absolute of int64 | Chunk of int | Percentage of float | TailOrCheckpoint

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
        /// Maximum number of stream readers to permit
        streamReaders: int
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
#if cosmos
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSource of string
        | [<AltCommandLine "-ad"; Unique>] LeaseCollectionDestination of string
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
#else
        | [<AltCommandLine "-b"; Unique>] MinBatchSize of int
        | [<AltCommandLine "-pre"; Unique>] MaxPending of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
        | [<AltCommandLine "-p"; Unique>] Position of int64
        | [<AltCommandLine "-c"; Unique>] Chunk of int
        | [<AltCommandLine "-P"; Unique>] Percent of float
        | [<AltCommandLine "-i"; Unique>] StreamReaders of int
        | [<AltCommandLine "-t"; Unique>] Tail of intervalS: float
        | [<AltCommandLine "-vc"; Unique>] VerboseConsole
#endif
        | [<AltCommandLine "-f"; Unique>] ForceRestart
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Source of ParseResults<SourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
#if cosmos
                | ForceStartFromHere _ ->   "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 1000"
                | LeaseCollectionSource _ ->"specify Collection Name for Leases collection, within `source` connection/database (default: `source`'s `collection` + `-aux`)."
                | LeaseCollectionDestination _ -> "specify Collection Name for Leases collection, within [destination] `cosmos` connection/database (default: defined relative to `source`'s `collection`)."
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | Source _ ->               "CosmosDb input parameters."
#else
                | ForceRestart _ ->         "Forget the current committed position; start from (and commit) specified position. Default: start from specified position or resume from committed."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | MinBatchSize _ ->         "minimum item count to drop down to in reaction to read failures. Default: 512"
                | MaxPending _ ->           "Maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "Maximum number of concurrent writes to target permitted. Default: 64"
                | Position _ ->             "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"
                | StreamReaders _ ->        "number of concurrent readers. Default: 8"
                | Tail _ ->                 "attempt to read from tail at specified interval in Seconds. Default: 1"
                | VerboseConsole ->         "request Verbose Console Logging. Default: off"
                | Source _ ->               "EventStore input parameters."
#endif
                | Verbose ->                "request Verbose Logging. Default: off"
    and Arguments(a : ParseResults<Parameters>) =
        member __.MaybeSeqEndpoint =    if a.Contains LocalSeq then Some "http://localhost:5341" else None
#if cosmos
        member __.LeaseId =             a.GetResult ConsumerGroupName
        member __.BatchSize =           a.GetResult(BatchSize,1000)
        member __.StartFromHere =       a.Contains ForceStartFromHere
        member __.LagFrequency =        a.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.ChangeFeedVerbose =   a.Contains ChangeFeedVerbose
#else
        member __.ConsumerGroupName =   a.GetResult ConsumerGroupName
        member __.VerboseConsole =      a.Contains VerboseConsole
        member __.ConsoleMinLevel =     if __.VerboseConsole then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
        member __.StartingBatchSize =   a.GetResult(BatchSize,4096)
        member __.MaxPendingBatches =   a.GetResult(MaxPending,64)
        member __.MaxWriters =          a.GetResult(MaxWriters,64)
        member __.MinBatchSize =        a.GetResult(MinBatchSize,512)
        member __.StreamReaders =       a.GetResult(StreamReaders,8)
        member __.TailInterval =        a.GetResult(Tail,1.) |> TimeSpan.FromSeconds
        member __.CheckpointInterval =  TimeSpan.FromHours 1.
        member __.ForceRestart =        a.Contains ForceRestart
#endif

        member __.Verbose =             a.Contains Verbose

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
                match a.TryGetResult Position, a.TryGetResult Chunk, a.TryGetResult Percent with
                | Some p, _, _ ->   Absolute p
                | _, Some c, _ ->   StartPos.Chunk c
                | _, _, Some p ->   Percentage p 
                | None, None, None -> StartPos.TailOrCheckpoint
            Log.Information("Processing Consumer Group {groupName} from {startPos} (force: {forceRestart}) in Database {db} Collection {coll}",
                x.ConsumerGroupName, startPos, x.ForceRestart, x.Destination.Database, x.Destination.Collection)
            Log.Information("Ingesting in batches of [{minBatchSize}..{batchSize}] with {stripes} stream readers",
                x.MinBatchSize, x.StartingBatchSize, x.StreamReaders)
            {   groupName = x.ConsumerGroupName; start = startPos; checkpointInterval = x.CheckpointInterval; tailInterval = x.TailInterval; forceRestart = x.ForceRestart
                batchSize = x.StartingBatchSize; minBatchSize = x.MinBatchSize; streamReaders = x.StreamReaders }
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
            | bad, [] ->    let black = Set.ofList bad in Log.Information("Excluding categories: {cats}", black); fun x -> not (black.Contains x)
            | [], good ->   let white = Set.ofList good in Log.Information("Only copying categories: {cats}", white); fun x -> white.Contains x
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
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->       "override the connection mode (default: DirectTcp)."
    and DestinationArguments(a : ParseResults<DestinationParameters>) =
        member __.Mode =                a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Discovery =           Discovery.FromConnectionString __.Connection
        member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

        member __.Timeout =             a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries, 1)
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
    type [<RequireQualifiedAccess; NoComparison>] CoordinationWork<'Pos> =
        | Result of CosmosIngester.Writer.Result
        | ProgressResult of Choice<int64,exn>
        | Unbatched of CosmosIngester.Batch
        | BatchWithTracking of 'Pos * CosmosIngester.Batch[]

    type TailAndPrefixesReader(conn, batchSize, minBatchSize, tryMapEvent: EventStore.ClientAPI.ResolvedEvent -> CosmosIngester.Batch option, maxDop, ?statsInterval) = 
        let sleepIntervalMs = 100
        let dop = new SemaphoreSlim(maxDop)
        let work = EventStoreSource.ReadQueue(batchSize, minBatchSize, ?statsInterval=statsInterval)
        member __.HasCapacity = work.QueueCount < dop.CurrentCount
        member __.AddTail(startPos, max, interval) = work.AddTail(startPos, max, interval)
        member __.AddStreamPrefix(stream, pos, len) = work.AddStreamPrefix(stream, pos, len)
        member __.Pump(postItem, shouldTail, postBatch) = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                work.OverallStats.DumpIfIntervalExpired()
                let! _ = dop.Await()
                let forkRunRelease task = async {
                    let! _ = Async.StartChild <| async {
                        try let! _ = work.Process(conn, tryMapEvent, postItem, shouldTail, postBatch, task) in ()
                        finally dop.Release() |> ignore }
                    return () }
                match work.TryDequeue() with
                | true, task ->
                    do! forkRunRelease task
                | false, _ ->
                    dop.Release() |> ignore
                    do! Async.Sleep sleepIntervalMs } 

    type StartMode = Starting | Resuming | Overridding
    type Coordinator(log : Serilog.ILogger, readers : TailAndPrefixesReader, cosmosContext, maxWriters, progressWriter: Checkpoint.ProgressWriter, maxPendingBatches, ?interval) =
        let statsIntervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
        let sleepIntervalMs = 10
        let input = new System.Collections.Concurrent.BlockingCollection<_>(System.Collections.Concurrent.ConcurrentQueue(), maxPendingBatches)
        let results = System.Collections.Concurrent.ConcurrentQueue()
        let buffer = CosmosIngester.StreamStates()
        let writers = CosmosIngester.Writers(CosmosIngester.Writer.write log cosmosContext, maxWriters)
        let tailSyncState = ProgressBatcher.State<EventStore.ClientAPI.Position>()
        // Yes, there is a race, but its constrained by the number of parallel readers and the fact that batches get ingested quickly here
        let mutable pendingBatchCount = 0
        let shouldThrottle () = pendingBatchCount > maxPendingBatches
        let mutable validatedEpoch, comittedEpoch : int64 option * int64 option = None, None
        let pumpReaders =
            let postWrite = (*we want to prioritize processing of catchup stream reads*) results.Enqueue << CoordinationWork.Unbatched 
            let postBatch pos xs = input.Add (CoordinationWork.BatchWithTracking (pos,xs))
            readers.Pump(postWrite, not << shouldThrottle, postBatch)
        let postWriteResult = results.Enqueue << CoordinationWork.Result
        let postProgressResult = results.Enqueue << CoordinationWork.ProgressResult
        member __.Pump() = async {
            use _ = writers.Result.Subscribe postWriteResult
            use _ = progressWriter.Result.Subscribe postProgressResult
            let! _ = Async.StartChild pumpReaders
            let! _ = Async.StartChild <| writers.Pump()
            let! _ = Async.StartChild <| progressWriter.Pump()
            let! ct = Async.CancellationToken
            let writerResultLog = log.ForContext<CosmosIngester.Writer.Result>()
            let mutable bytesPended, bytesPendedAgg = 0L, 0L
            let workPended, eventsPended, cycles = ref 0, ref 0, ref 0
            let rateLimited, timedOut, tooLarge, malformed = ref 0, ref 0, ref 0, ref 0
            let resultOk, resultDup, resultPartialDup, resultPrefix, resultExn = ref 0, ref 0, ref 0, ref 0, ref 0
            let progCommitFails, progCommits = ref 0, ref 0
            let badCats = CosmosIngester.CatStats()
            let dumpStats () =
                let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix + !resultExn
                bytesPendedAgg <- bytesPendedAgg + bytesPended
                Log.Information("Cycles {cycles} Queued {queued} reqs {events} events {mb:n}MB ∑{gb:n3}GB",
                    !cycles, !workPended, !eventsPended, mb bytesPended, mb bytesPendedAgg / 1024.)
                cycles := 0; workPended := 0; eventsPended := 0; bytesPended <- 0L

                buffer.Dump log

                Log.Information("Wrote {completed} ({ok} ok {dup} redundant {partial} partial {prefix} Missing {exns} Exns)",
                    results, !resultOk, !resultDup, !resultPartialDup, !resultPrefix, !resultExn)
                resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; resultExn := 0;
                if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 then
                    Log.Warning("Exceptions {rateLimited} rate-limited, {timedOut} timed out, {tooLarge} too large, {malformed} malformed",
                        !rateLimited, !timedOut, !tooLarge, !malformed)
                    rateLimited := 0; timedOut := 0; tooLarge := 0; malformed := 0 
                    if badCats.Any then Log.Error("Malformed categories {badCats}", badCats.StatsDescending); badCats.Clear()

                if !progCommitFails <> 0 || !progCommits <> 0 then
                    match comittedEpoch with
                    | None ->
                        log.Error("Progress @ {validated}; writing failing: {failures} failures ({commits} successful commits) Uncomitted {pendingBatches}/{maxPendingBatches}",
                                Option.toNullable validatedEpoch, !progCommitFails, !progCommits, pendingBatchCount, maxPendingBatches)
                    | Some committed when !progCommitFails <> 0 ->
                        log.Warning("Progress @ {validated} (committed: {committed}, {commits} commits, {failures} failures) Uncomitted {pendingBatches}/{maxPendingBatches}",
                                Option.toNullable validatedEpoch, committed, !progCommits, !progCommitFails, pendingBatchCount, maxPendingBatches)
                    | Some committed ->
                        log.Information("Progress @ {validated} (committed: {committed}, {commits} commits) Uncomitted {pendingBatches}/{maxPendingBatches}",
                                Option.toNullable validatedEpoch, committed, !progCommits, pendingBatchCount, maxPendingBatches)
                    progCommits := 0; progCommitFails := 0
                else
                    log.Information("Progress @ {validated} (committed: {committed}) Uncomitted {pendingBatches}/{maxPendingBatches}",
                        Option.toNullable validatedEpoch, Option.toNullable comittedEpoch, pendingBatchCount, maxPendingBatches)
            let tryDumpStats = every statsIntervalMs dumpStats
            let handle = function
                | CoordinationWork.Unbatched item ->
                    buffer.Add item |> ignore
                | CoordinationWork.BatchWithTracking(pos, items) ->
                    for item in items do
                        buffer.Add item |> ignore
                    tailSyncState.AppendBatch(pos, [|for x in items -> x.stream, x.span.index + int64 x.span.events.Length |])
                | CoordinationWork.Result res ->
                    match res with
                    | CosmosIngester.Writer.Result.Ok _ -> incr resultOk
                    | CosmosIngester.Writer.Result.Duplicate _ -> incr resultDup
                    | CosmosIngester.Writer.Result.PartialDuplicate _ -> incr resultPartialDup
                    | CosmosIngester.Writer.Result.PrefixMissing _ -> incr resultPrefix
                    | CosmosIngester.Writer.Result.Exn _ -> incr resultExn

                    let (stream, updatedState), kind = buffer.HandleWriteResult res
                    match updatedState.write with None -> () | Some wp -> tailSyncState.MarkStreamProgress(stream, wp)
                    match kind with
                    | CosmosIngester.Ok -> res.WriteTo writerResultLog
                    | CosmosIngester.RateLimited -> incr rateLimited
                    | CosmosIngester.TooLarge -> category stream |> badCats.Ingest; incr tooLarge
                    | CosmosIngester.Malformed -> category stream |> badCats.Ingest; incr malformed
                    | CosmosIngester.TimedOut -> incr timedOut
                | CoordinationWork.ProgressResult (Choice1Of2 epoch) ->
                    incr progCommits
                    comittedEpoch <- Some epoch
                | CoordinationWork.ProgressResult (Choice2Of2 (_exn : exn)) ->
                    incr progCommitFails
            let queueWrite (w : CosmosIngester.Batch) =
                incr workPended
                eventsPended := !eventsPended + w.span.events.Length
                bytesPended <- bytesPended + int64 (Array.sumBy CosmosIngester.cosmosPayloadBytes w.span.events)
                writers.Enqueue w
            while not ct.IsCancellationRequested do
                incr cycles
                // 1. propagate read items to buffer; propagate write write results to buffer and progress write impacts to local state
                let mutable more, gotWork = true, false
                while more do
                    match results.TryDequeue() with
                    | true, item -> handle item; gotWork <- true
                    | false, _ -> more <- false
                // 2. Mark off any progress achieved (releasing memory and/or or unblocking reading of batches)
                let (_validatedPos, _pendingBatchCount) = tailSyncState.Validate buffer.TryGetStreamWritePos
                pendingBatchCount <- _pendingBatchCount
                validatedEpoch <- _validatedPos |> Option.map (fun x -> x.CommitPosition)
                // 3. Feed latest position to store
                validatedEpoch |> Option.iter progressWriter.Post
                // 4. Enqueue streams with gaps if there is capacity (not overloading, to avoid redundant work)
                let mutable more = true 
                while more && readers.HasCapacity do
                    match buffer.TryGap() with
                    | Some (stream,pos,len) -> readers.AddStreamPrefix(stream,pos,len)
                    | None -> more <- false
                // 5. After that, [over] provision writers queue
                let mutable more = true
                while more && writers.HasCapacity do
                    match buffer.TryReady(writers.IsStreamBusy) with
                    | Some w -> queueWrite w; gotWork <- true
                    | None -> (); more <- false
                // 6. OK, we've stashed and cleaned work; now take some inputs
                let x = Stopwatch.StartNew()
                let mutable more = true
                while more && x.ElapsedMilliseconds < int64 sleepIntervalMs do
                    match input.TryTake() with
                    | true, item -> handle item
                    | false, _ -> more <- false
                match sleepIntervalMs - int x.ElapsedMilliseconds with
                | d when d > 0 ->
                    if writers.HasCapacity || gotWork then do! Async.Sleep 1
                    else do! Async.Sleep d
                | _ -> ()
                // 7. Periodically emit status info
                tryDumpStats () }

        static member Run (log : Serilog.ILogger) (conn, spec, tryMapEvent) (maxWriters, cosmosContext, maxPendingBatches) resolveCheckpointStream = async {
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
                Log.Information("Sync {mode} {groupName} @ {pos} (chunk {chunk}, {pct:p1}) tailing every {interval}s, checkpointing every {checkpointFreq}m",
                    startMode, spec.groupName, startPos.CommitPosition, EventStoreSource.chunk startPos,
                    float startPos.CommitPosition/float max.CommitPosition, spec.tailInterval.TotalSeconds, checkpointFreq.TotalMinutes)
                return startPos }
            let readers = TailAndPrefixesReader(conn, spec.batchSize, spec.minBatchSize, tryMapEvent, spec.streamReaders + 1)
            readers.AddTail(startPos, max, spec.tailInterval)
            let progress = Checkpoint.ProgressWriter(checkpoints.Commit)
            let coordinator = Coordinator(log, readers, cosmosContext, maxWriters, progress, maxPendingBatches=maxPendingBatches)
            do! coordinator.Pump() }
#else
module CosmosSource =
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing

    type [<RequireQualifiedAccess; NoEquality; NoComparison>] CoordinationWork<'Pos> =
        | Result of CosmosIngester.Writer.Result
        | ProgressResult of Choice<int,exn>
        | BatchWithTracking of 'Pos * CosmosIngester.Batch[]

    /// Manages writing of progress
    /// - Each write attempt is of the newest token
    /// - retries until success or a new item is posted
    type ProgressWriter() =
        let pumpSleepMs = 100
        let mutable lastCompleted = -1
        let mutable latest = None
        let result = Event<_>()
        [<CLIEvent>] member __.Result = result.Publish
        member __.Post(version,f) = latest <- Some (version,f) 
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                match latest with
                | Some (v,f) when v <> lastCompleted ->
                    try do! f
                        lastCompleted <- v
                        result.Trigger (Choice1Of2 v)
                    with e -> result.Trigger (Choice2Of2 e)
                | _ -> do! Async.Sleep pumpSleepMs }
    type PendingWork = { batches : int; streams : int }
    type Coordinator private (cosmosContext, cts : CancellationTokenSource, ?maxWriters, ?interval) =
        let pumpSleepMs = 100
        let maxWriters = defaultArg maxWriters 256
        let statsIntervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
        let work = System.Collections.Concurrent.ConcurrentQueue()
        let buffer = CosmosIngester.StreamStates()
        let progressWriter = ProgressWriter()
        let syncState = ProgressBatcher.State()
        let mutable epoch = 0
        let postBatch pos xs =
            let batchStamp = Interlocked.Increment &epoch
            work.Enqueue(CoordinationWork.BatchWithTracking ((batchStamp,pos),xs))
        let postWriteResult = work.Enqueue << CoordinationWork.Result
        let postWriteProgressResult = work.Enqueue << CoordinationWork.ProgressResult
        let mutable pendingBatches = 0
        member __.Pump(log : ILogger) = async {
            let writers = CosmosIngester.Writers(CosmosIngester.Writer.write log cosmosContext, maxWriters)
            let writerResultLog = log.ForContext<CosmosIngester.Writer.Result>()
            use _ = writers.Result.Subscribe postWriteResult
            use _ = progressWriter.Result.Subscribe postWriteProgressResult
            let! _ = Async.StartChild <| writers.Pump()
            let! _ = Async.StartChild <| progressWriter.Pump()
            let! ct = Async.CancellationToken
            let mutable bytesPended = 0L
            let resultsHandled, workPended, eventsPended = ref 0, ref 0, ref 0
            let rateLimited, timedOut, malformed = ref 0, ref 0, ref 0
            let progressFailed, progressWritten = ref 0, ref 0
            let mutable progressEpoch = None
            let badCats = CosmosIngester.CatStats()
            let dumpStats () =
                if !rateLimited <> 0 || !timedOut <> 0 || !malformed <> 0 then
                    log.Warning("Writer Exceptions {rateLimited} rate-limited, {timedOut} timed out, {malformed} malformed",!rateLimited, !timedOut, !malformed)
                    rateLimited := 0; timedOut := 0; malformed := 0 
                    if badCats.Any then log.Error("Malformed categories {badCats}", badCats.StatsDescending); badCats.Clear()
                let tl = if !workPended = 0 && !eventsPended = 0 && !resultsHandled = 0 then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                log.Write(tl, "Writer Throughput {queued} requests, {events} events; Completed {completed} reqs; Egress {gb:n3}GB",
                    !workPended, !eventsPended,!resultsHandled, mb bytesPended / 1024.)
                resultsHandled := 0; workPended := 0; eventsPended := 0 
                if !progressFailed <> 0 || !progressWritten <> 0 then
                    match progressEpoch with
                    | None -> log.Error("Progress writing failing: {failures} failures", !progressWritten, !progressFailed)
                    | Some epoch ->
                        if !progressFailed <> 0 then log.Warning("Progress Epoch {epoch} ({updates} updates, {failures} failures", epoch, !progressWritten, !progressFailed)
                        else log.Information("Progress Epoch {epoch} ({updates} updates)", epoch, !progressWritten)
                    progressFailed := 0; progressWritten := 0
                buffer.Dump log
            let tryDumpStats = every statsIntervalMs dumpStats
            let handle = function
                | CoordinationWork.BatchWithTracking(pos, items) ->
                    for item in items do
                        buffer.Add item |> ignore
                    syncState.AppendBatch(pos, [|for x in items -> x.stream, x.span.index + int64 x.span.events.Length |])
                | CoordinationWork.ProgressResult (Choice1Of2 epoch) ->
                    incr progressWritten
                    progressEpoch <- Some epoch
                | CoordinationWork.ProgressResult (Choice2Of2 (_exn : exn)) ->
                    incr progressFailed
                | CoordinationWork.Result res ->
                    incr resultsHandled
                    let (stream, updatedState), kind = buffer.HandleWriteResult res
                    match updatedState.write with None -> () | Some wp -> syncState.MarkStreamProgress(stream, wp)
                    match kind with
                    | CosmosIngester.Ok -> res.WriteTo writerResultLog
                    | CosmosIngester.RateLimited -> incr rateLimited
                    | CosmosIngester.Malformed -> category stream |> badCats.Ingest; incr malformed
                    | CosmosIngester.TimedOut -> incr timedOut
            let queueWrite (w : CosmosIngester.Batch) =
                incr workPended
                eventsPended := !eventsPended + w.span.events.Length
                bytesPended <- bytesPended + int64 (Array.sumBy CosmosIngester.cosmosPayloadBytes w.span.events)
                writers.Enqueue w
            while not ct.IsCancellationRequested do
                // 1. propagate read items to buffer; propagate write results to buffer + Progress 
                match work.TryDequeue() with
                | true, item ->
                    handle item
                | false, _ ->
                    let validatedPos, pendingBatchCount = syncState.Validate buffer.TryGetStreamWritePos
                    pendingBatches <- pendingBatchCount
                    // 2. After that, [over] provision writers queue
                    let mutable more = writers.HasCapacity
                    while more do
                        match buffer.TryReady(writers.IsStreamBusy) with
                        | Some w -> queueWrite w; more <- writers.HasCapacity
                        | None -> more <- false
                    // 3. Periodically emit status info
                    tryDumpStats ()
                    // 4. Feed latest state to progress writer
                    validatedPos |> Option.iter progressWriter.Post
                    // 5. Sleep if we've nothing else to do
                    do! Async.Sleep pumpSleepMs
            dumpStats ()
            log.Warning("... Coordinator exiting") }

        static member Start(log : Serilog.ILogger, cosmosContext) : Coordinator =
            let cts = new CancellationTokenSource()
            let coordinator = new Coordinator(cosmosContext, cts)
            Async.Start(coordinator.Pump log,cts.Token)
            coordinator
        member __.Submit(checkpoint : Async<unit>, batches : CosmosIngester.Batch[]) = postBatch checkpoint batches
        member __.PendingBatches = pendingBatches
        interface IDisposable with member __.Dispose() = cts.Cancel()

    let createRangeSyncHandler (log:ILogger) (ctx: Core.CosmosContext) (transform : Microsoft.Azure.Documents.Document -> CosmosIngester.Batch seq) =
        let busyPauseMs = 500
        let maxUnconfirmedBatches = 10
        let mutable coordinator = Unchecked.defaultof<_>
        let init rangeLog =
            coordinator <- Coordinator.Start(rangeLog, ctx)
        let ingest docs checkpoint : (*streams*)int * (*events*)int =
            let events = docs |> Seq.collect transform |> Array.ofSeq
            coordinator.Submit(checkpoint,events)
            events.Length, HashSet(seq { for x in events -> x.stream }).Count
        let dispose () = (coordinator :> IDisposable).Dispose()
        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let processBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            // Pass along the function that the coordinator will run to checkpoint past this batch when such progress has been achieved
            let checkpoint = async { do! ctx.CheckpointAsync() |> Async.AwaitTaskCorrect }
            let pt, (events,streams) = Stopwatch.Time(fun () -> ingest docs checkpoint)
            log.Information("Read -{token,6} {count,4} docs {requestCharge,6}RU {l:n1}s Gen {events,5} events {p:n3}s Sync {streams,5} streams",
                ctx.FeedResponse.ResponseContinuation.Trim[|'"'|], docs.Count, (let c = ctx.FeedResponse.RequestCharge in c.ToString("n1")),
                float sw.ElapsedMilliseconds / 1000., events, (let e = pt.Elapsed in e.TotalSeconds), streams)
            // Only hand back control to the CFP iff our processing backlog is under control
            // no point getting too far ahead and/or overloading ourselves if we can't log our progress
            let mutable first = true
            let pauseTimer = Stopwatch.StartNew()
            let backlogIsExcessive pendingBatches =
                let tooMuch = pendingBatches >= maxUnconfirmedBatches
                if first && tooMuch then log.Information("Pausing due to backlog of incomplete batches...")
                let longDelay = pauseTimer.ElapsedMilliseconds > 5000L
                let level = if tooMuch && (first || longDelay) then Events.LogEventLevel.Information else Events.LogEventLevel.Debug
                first <- false
                if longDelay then pauseTimer.Reset()
                log.Write(level, "Pending Batches {pb}", pendingBatches)
                tooMuch
            while backlogIsExcessive coordinator.PendingBatches do
                do! Async.Sleep busyPauseMs
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

    let transformV0 catFilter (v0SchemaDocument: Document) : CosmosIngester.Batch seq = seq {
        let parsed = EventV0Parser.parse v0SchemaDocument
        let streamName = (*if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+*)parsed.Stream
        if catFilter (category streamName) then
            yield { stream = streamName; span = { index = parsed.Index; events = [| parsed |] } } }
    //#else
    let transformOrFilter catFilter (changeFeedDocument: Document) : CosmosIngester.Batch seq = seq {
        for e in DocumentParser.enumEvents changeFeedDocument do
            if catFilter (category e.Stream) then
                // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
                yield { stream = e.Stream; span = { index = e.Index; events = [| e |] } } }
    //#endif
#endif

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    module RuCounters =
        open Serilog.Events
        open Equinox.Cosmos.Store

        let inline (|Stats|) ({ interval = i; ru = ru }: Log.Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds

        let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|) = function
            | Log.Tip (Stats s)
            | Log.TipNotFound (Stats s)
            | Log.TipNotModified (Stats s)
            | Log.Query (_,_, (Stats s)) -> CosmosReadRc s
            // slices are rolled up into batches so be sure not to double-count
            | Log.Response (_,(Stats s)) -> CosmosResponseRc s
            | Log.SyncSuccess (Stats s)
            | Log.SyncConflict (Stats s) -> CosmosWriteRc s
            | Log.SyncResync (Stats s) -> CosmosResyncRc s
        let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
            | (:? ScalarValue as x) -> Some x.Value
            | _ -> None
        let (|CosmosMetric|_|) (logEvent : LogEvent) : Log.Event option =
            match logEvent.Properties.TryGetValue("cosmosEvt") with
            | true, SerilogScalar (:? Log.Event as e) -> Some e
            | _ -> None
        type RuCounter =
            { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
            static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
            member __.Ingest (ru, ms) =
                System.Threading.Interlocked.Increment(&__.count) |> ignore
                System.Threading.Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
                System.Threading.Interlocked.Add(&__.ms, ms) |> ignore
        type RuCounterSink() =
            static member val Read = RuCounter.Create() with get, set
            static member val Write = RuCounter.Create() with get, set
            static member val Resync = RuCounter.Create() with get, set
            static member Reset() =
                RuCounterSink.Read <- RuCounter.Create()
                RuCounterSink.Write <- RuCounter.Create()
                RuCounterSink.Resync <- RuCounter.Create()
            interface Serilog.Core.ILogEventSink with
                member __.Emit logEvent = logEvent |> function
                    | CosmosMetric (CosmosReadRc stats) -> RuCounterSink.Read.Ingest stats
                    | CosmosMetric (CosmosWriteRc stats) -> RuCounterSink.Write.Ingest stats
                    | CosmosMetric (CosmosResyncRc stats) -> RuCounterSink.Resync.Ingest stats
                    | _ -> ()

        let dumpStats duration (log: Serilog.ILogger) =
            let stats =
              [ "Read", RuCounterSink.Read
                "Write", RuCounterSink.Write
                "Resync", RuCounterSink.Resync ]
            let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
            let logActivity name count rc lat =
                log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                    name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
            for name, stat in stats do
                let ru = float stat.rux100 / 100.
                totalCount <- totalCount + stat.count
                totalRc <- totalRc + ru
                totalMs <- totalMs + stat.ms
                logActivity name stat.count ru stat.ms
            logActivity "TOTAL" totalCount totalRc totalMs
            let measures : (string * (TimeSpan -> float)) list =
              [ "s", fun x -> x.TotalSeconds
                "m", fun x -> x.TotalMinutes
                "h", fun x -> x.TotalHours ]
            let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)
        let startTaskToDumpStatsEvery (freq : TimeSpan) =
            let rec aux () = async {
                dumpStats freq Log.Logger
                do! Async.Sleep (int freq.TotalMilliseconds)
                return! aux () }
            Async.Start(aux ())
    let initialize verbose changeLogVerbose maybeSeqEndpoint =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
                        let cfpLevel = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpLevel)
            |> fun c -> let ingesterLevel = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Information
                        c.MinimumLevel.Override(typeof<CosmosIngester.Writers>.FullName, ingesterLevel)
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let generalLevel = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<CosmosIngester.Writer.Result>.FullName, generalLevel)
                         .MinimumLevel.Override(typeof<Checkpoint.CheckpointSeries>.FullName, generalLevel)
                         .MinimumLevel.Override(typeof<CosmosContext>.FullName, generalLevel)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            |> fun c -> c.WriteTo.Sink(RuCounters.RuCounterSink())
            |> fun c -> c.CreateLogger()
        RuCounters.startTaskToDumpStatsEvery (TimeSpan.FromMinutes 1.)
        Log.ForContext<CosmosIngester.Writers>()

[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
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
        let target = Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.ForContext<Core.CosmosContext>())
#if cosmos
        let log = Logging.initialize args.Verbose args.ChangeFeedVerbose args.MaybeSeqEndpoint
        let discovery, source, connectionPolicy, catFilter = args.Source.BuildConnectionDetails()
        let auxDiscovery, aux, leaseId, startFromHere, batchSize, lagFrequency = args.BuildChangeFeedParams()
#if marveleqx
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#else
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformOrFilter catFilter)
        // Uncomment to test marveleqx mode
        // let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#endif
        CosmosSource.run (discovery, source) (auxDiscovery, aux) connectionPolicy
            (leaseId, startFromHere, batchSize, lagFrequency)
            createSyncHandler
#else
        let log = Logging.initialize args.Verbose args.VerboseConsole args.MaybeSeqEndpoint
        let esConnection = args.Source.Connect(log, log, ConnectionStrategy.ClusterSingle NodePreference.PreferSlave)
        let catFilter = args.Source.CategoryFilterFunction
        let spec = args.BuildFeedParams()
        let tryMapEvent catFilter (x : EventStore.ClientAPI.ResolvedEvent) =
            match x.Event with
            | e when not e.IsJson
                || e.EventStreamId.StartsWith("$") 
                || e.EventType.StartsWith("compacted",StringComparison.OrdinalIgnoreCase)
                || e.EventStreamId.EndsWith("_checkpoints")
                || e.EventStreamId.StartsWith("InventoryLog") // 5GB, causes lopsided partitions, unused
                || e.EventStreamId = "ReloadBatchId" // does not start at 0
                || e.EventStreamId = "PurchaseOrder-5791" // Too large
                || e.EventStreamId.EndsWith("_checkpoint")
                || not (catFilter e.EventStreamId) -> None
            | e -> EventStoreSource.tryToBatch e
        EventStoreSource.Coordinator.Run log (esConnection.ReadConnection, spec, tryMapEvent catFilter) (args.MaxWriters, target, args.MaxPendingBatches) resolveCheckpointStream
#endif
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.InvalidArguments msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1