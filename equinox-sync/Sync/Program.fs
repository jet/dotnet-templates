module SyncTemplate.Program

open Equinox.Cosmos
open Equinox.Cosmos.Core
open Equinox.Cosmos.Projection
open Equinox.Store
open Serilog
open System
open System.Diagnostics
open System.Threading

let mb x = float x / 1024. / 1024.
let category (streamName : string) = streamName.Split([|'-'|],2).[0]
let arrayBytes (x:byte[]) = if x = null then 0 else x.Length

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
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSource of string
        | [<AltCommandLine "-ad"; Unique>] LeaseCollectionDestination of string
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        | [<AltCommandLine "-f"; Unique>] ForceStartFromHere
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Source of ParseResults<SourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | ForceStartFromHere _ ->   "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 1000"
                | LeaseCollectionSource _ ->"specify Collection Name for Leases collection, within `source` connection/database (default: `source`'s `collection` + `-aux`)."
                | LeaseCollectionDestination _ -> "specify Collection Name for Leases collection, within [destination] `cosmos` connection/database (default: defined relative to `source`'s `collection`)."
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | Source _ ->               "CosmosDb input parameters."
                | Verbose ->                "request Verbose Logging. Default: off"
    and Arguments(a : ParseResults<Parameters>) =
        member __.LeaseId =             a.GetResult ConsumerGroupName
        member __.BatchSize =           a.GetResult(BatchSize,1000)
        member __.StartFromHere =       a.Contains ForceStartFromHere
        member __.LagFrequency =        a.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.ChangeFeedVerbose =   a.Contains ChangeFeedVerbose

        member __.Verbose =             a.Contains Verbose

        member val Source : SourceArguments = SourceArguments(a.GetResult Source)
        member __.Destination : DestinationArguments = __.Source.Destination
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
    and [<NoEquality; NoComparison>] SourceParameters =
        | [<AltCommandLine "-m">] SourceConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">] SourceTimeout of float
        | [<AltCommandLine "-r">] SourceRetries of int
        | [<AltCommandLine "-rt">] SourceRetriesWaitTime of int
        | [<AltCommandLine "-s">] SourceConnection of string
        | [<AltCommandLine "-d">] SourceDatabase of string
        | [<AltCommandLine "-c"; Unique(*Mandatory is not supported*)>] SourceCollection of string
        | [<AltCommandLine "-e">] CategoryBlacklist of string
        | [<AltCommandLine "-i">] CategoryWhitelist of string
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<DestinationParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | SourceConnection _ ->     "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION)."
                | SourceDatabase _ ->       "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE)."
                | SourceCollection _ ->     "specify a collection name within `SourceDatabase`."
                | SourceTimeout _ ->        "specify operation timeout in seconds (default: 5)."
                | SourceRetries _ ->        "specify operation retries (default: 1)."
                | SourceRetriesWaitTime _ ->"specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | SourceConnectionMode _ -> "override the connection mode (default: DirectTcp)."
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

module Progress =
    open System.Collections.Generic

    type [<NoComparison>] internal Chunk<'Pos> = { pos: 'Pos; streamToRequiredIndex : Dictionary<string,int64> }

    type State<'Pos>(?currentPos : 'Pos) =
        let pending = Queue<_>()
        let mutable validatedPos = currentPos
        member __.AppendBatch(pos, streamWithRequiredIndices : (string * int64) seq) =
            let byStream = streamWithRequiredIndices |> Seq.groupBy fst |> Seq.map (fun (s,xs) -> KeyValuePair(s,xs |> Seq.map snd |> Seq.max))
            pending.Enqueue { pos = pos; streamToRequiredIndex = Dictionary byStream }
        member __.MarkStreamProgress(stream, index) =
            for x in pending do
                match x.streamToRequiredIndex.TryGetValue stream with
                | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
                | _, _ -> ()
            let headIsComplete () = match pending.TryPeek() with true, head -> Seq.isEmpty head.streamToRequiredIndex | _ -> false
            while headIsComplete () do
                let headBatch = pending.Dequeue()
                validatedPos <- Some headBatch.pos
        member __.Validate tryGetStreamWritePos : 'Pos option * int =
            let rec aux () =
                match pending.TryPeek() with
                | false, _ -> ()
                | true, batch ->
                    for KeyValue (stream, requiredIndex) in Array.ofSeq batch.streamToRequiredIndex do
                        match tryGetStreamWritePos stream with
                        | Some currentPos when currentPos >= requiredIndex -> batch.streamToRequiredIndex.Remove stream |> ignore
                        | _ -> ()
                    if batch.streamToRequiredIndex.Count = 0 then
                        let headBatch = pending.Dequeue()
                        validatedPos <- Some headBatch.pos
                        aux ()
            aux ()
            validatedPos, pending.Count

module CosmosIngester =
    open Equinox.Cosmos.Store

    let cosmosPayloadLimit = 2 * 1024 * 1024 - 1024
    let inline cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + 4

    type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
    type [<NoComparison>] Batch = { stream: string; span: Span }

    module Writer =
        type [<NoComparison>] Result =
            | Ok of stream: string * updatedPos: int64
            | Duplicate of stream: string * updatedPos: int64
            | PartialDuplicate of overage: Batch
            | PrefixMissing of batch: Batch * writePos: int64
            | Exn of exn: exn * batch: Batch
            member __.WriteTo(log: ILogger) =
                match __ with
                | Ok (stream, pos) ->
                    log.Information("Wrote     {stream} up to {pos}", stream, pos)
                | Duplicate (stream, pos) ->
                    log.Information("Ignored   {stream} (synced up to {pos})", stream, pos)
                | PartialDuplicate  overage ->
                    log.Information("Requeing  {stream} {pos} ({count} events)", overage.stream, overage.span.index, overage.span.events.Length)
                | PrefixMissing ({stream=stream; span=span},pos) ->
                    log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, span.index-pos, span.events.Length, span.index)
                | Exn (exn, batch) ->
                    log.Warning(exn,"Writing   {stream} failed, retrying {count} events ....", batch.stream, batch.span.events.Length)
        let write (log : ILogger) (ctx : CosmosContext) ({ stream = s; span = { index = p; events = e}} as batch) = async {
            let stream = ctx.CreateStream s
            log.Debug("Writing {s}@{i}x{n}",s,p,e.Length)
            try let! res = ctx.Sync(stream, { index = p; etag = None }, e)
                match res with
                | AppendResult.Ok pos -> return Ok (s, pos.index) 
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index, p + e.LongLength with
                    | actual, expectedMax when actual >= expectedMax -> return Duplicate (s, pos.index)
                    | actual, _ when p > actual -> return PrefixMissing (batch, actual)
                    | actual, _ ->
                        log.Debug("pos {pos} batch.pos {bpos} len {blen} skip {skip}", actual, p, e.LongLength, actual-p)
                        return PartialDuplicate { stream = s; span = { index = actual; events = e |> Array.skip (actual-p |> int) } }
            with e -> return Exn (e, batch) }

    module Queue =
        module Span =
            let private (|Max|) x = x.index + x.events.LongLength
            let private trim min (Max m as x) =
                // Full remove
                if m <= min then { index = min; events = [||] }
                // Trim until min
                elif m > min && x.index < min then { index = min; events = x.events |> Array.skip (min - x.index |> int) }
                // Leave it
                else x
            let merge min (xs : Span seq) =
                let buffer = ResizeArray()
                let mutable curr = { index = min; events = [||]}
                for x in xs |> Seq.sortBy (fun x -> x.index) do
                    match curr, trim min x with
                    // no data incoming, skip
                    | _, x when x.events.Length = 0 ->
                        ()
                    // Not overlapping, no data buffered -> buffer
                    | c, x when c.events.Length = 0 ->
                        curr <- x
                    // Overlapping, join
                    | Max cMax as c, x when cMax >= x.index ->
                        curr <- { c with events = Array.append c.events (trim cMax x).events }
                    // Not overlapping, new data
                    | c, x ->
                        buffer.Add c
                        curr <- x
                if curr.events.Length <> 0 then buffer.Add curr
                if buffer.Count = 0 then null else buffer.ToArray()

        type [<NoComparison>] StreamState = { write: int64 option; queue: Span[] } with
            member __.IsReady =
                if __.queue = null then false
                else
                    match __.write, Array.tryHead __.queue with
                    | Some w, Some { index = i } -> i = w
                    | None, _ -> true
                    | _ -> false
            member __.Size =
                if __.queue = null then 0
                else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy (fun x -> arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length*2 + 16)
        module StreamState =
            let inline optionCombine f (r1: int64 option) (r2: int64 option) =
                match r1, r2 with
                | Some x, Some y -> f x y |> Some
                | None, None -> None
                | None, x | x, None -> x
            let combine (s1: StreamState) (s2: StreamState) : StreamState =
                let writePos = optionCombine max s1.write s2.write
                let items = seq { if s1.queue <> null then yield! s1.queue; if s2.queue <> null then yield! s2.queue }
                { write = writePos; queue = Span.merge (defaultArg writePos 0L) items}

        let (|TimedOutMessage|RateLimitedMessage|MalformedMessage|Other|) (e: exn) =
            match string e with
            | m when m.Contains "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
            | m when m.Contains "Microsoft.Azure.Documents.RequestRateTooLargeException" -> RateLimitedMessage
            | m when m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
                 || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position" -> MalformedMessage
            | _ -> Other

        /// Gathers stats relating to how many items of a given category have been observed
        type CatStats() =
            let cats = System.Collections.Generic.Dictionary<string,int>()
            member __.Ingest(cat,?weight) = 
                let weight = defaultArg weight 1
                match cats.TryGetValue cat with
                | true, catCount -> cats.[cat] <- catCount + weight
                | false, _ -> cats.[cat] <- weight
            member __.Any = cats.Count <> 0
            member __.Clear() = cats.Clear()
            member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd

        type Result = TimedOut | RateLimited | Malformed | Ok
        type StreamStates() =
            let states = System.Collections.Generic.Dictionary<string, StreamState>()
            let dirty = System.Collections.Generic.Queue()
            let markDirty stream = if (not << dirty.Contains) stream then dirty.Enqueue stream
            
            let update stream (state : StreamState) =
                match states.TryGetValue stream with
                | false, _ ->
                    states.Add(stream, state)
                    markDirty stream
                    stream, state
                | true, current ->
                    let updated = StreamState.combine current state
                    states.[stream] <- updated
                    if updated.IsReady then markDirty stream
                    stream, updated
            let updateWritePos stream pos span =
                update stream { write = pos; queue = span }

            member __.Add(item: Batch) = updateWritePos item.stream None [|item.span|]
            member __.TryGetStreamWritePos stream = match states.TryGetValue stream with true, value -> value.write | _ -> None
            member __.HandleWriteResult = function
                | Writer.Result.Ok (stream, pos) -> updateWritePos stream (Some pos) null, Ok
                | Writer.Result.Duplicate (stream, pos) -> updateWritePos stream (Some pos) null, Ok
                | Writer.Result.PartialDuplicate overage -> updateWritePos overage.stream (Some overage.span.index) [|overage.span|], Ok
                | Writer.Result.PrefixMissing (overage,pos) ->
                    updateWritePos overage.stream (Some pos) [|overage.span|], Ok
                | Writer.Result.Exn (exn, batch) ->
                    let r = 
                        match exn with
                        | RateLimitedMessage -> RateLimited
                        | TimedOutMessage -> TimedOut
                        | MalformedMessage -> Malformed
                        | Other -> Ok
                    __.Add(batch), r
            member __.TryReady() =
                let rec aux () =
                    match dirty |> Queue.tryDequeue with
                    | None -> None
                    | Some stream ->

                    match states.[stream] with
                    | s when not s.IsReady -> aux ()
                    | state ->
                        let h = state.queue |> Array.head
                    
                        let mutable bytesBudget = cosmosPayloadLimit
                        let mutable count = 0 
                        let max2MbMax100EventsMax10EventsFirstTranche (y : Equinox.Codec.IEvent<byte[]>) =
                            bytesBudget <- bytesBudget - cosmosPayloadBytes y
                            count <- count + 1
                            // Reduce the item count when we don't yet know the write position
                            count <= (if Option.isNone state.write then 10 else 100) && (bytesBudget >= 0 || count = 1)
                        Some { stream = stream; span = { index = h.index; events = h.events |> Array.takeWhile max2MbMax100EventsMax10EventsFirstTranche } }
                aux ()
            member __.Dump(log : ILogger) =
                let mutable synced, ready, waiting = 0, 0, 0
                let mutable readyB, waitingB = 0L, 0L
                let waitCats, readyCats, readyStreams = CatStats(), CatStats(), CatStats()
                for KeyValue (stream,state) in states do
                    match int64 state.Size with
                    | 0L ->
                        synced <- synced + 1
                    | sz when state.IsReady ->
                        readyCats.Ingest(category stream)
                        readyStreams.Ingest(sprintf "%s@%A" stream state.write, int sz)
                        ready <- ready + 1
                        readyB <- readyB + sz
                    | sz ->
                        waitCats.Ingest(category stream)
                        waiting <- waiting + 1
                        waitingB <- waitingB + sz
                log.Information("Streams Synced {synced} Dirty {dirty} Ready {ready}/{readyMb:n1}MB Awaiting prefix {waiting}/{waitingMb:n1}MB",
                    synced, dirty.Count, ready, mb readyB, waiting, mb waitingB)
                if waitCats.Any then log.Warning("Waiting {waitCats}", waitCats.StatsDescending)
                if readyCats.Any then log.Information("Ready {readyCats} {readyStreams}", readyCats.StatsDescending, readyStreams.StatsDescending)

    type Writers(write, maxDop) =
        let work = System.Collections.Concurrent.ConcurrentQueue()
        let result = Event<_>()
        [<CLIEvent>] member __.Result = result.Publish
        member __.Enqueue item = work.Enqueue item
        member __.HasCapacity = work.Count < maxDop
        member __.Pump() = async {
            let dop = new SemaphoreSlim(maxDop)
            let dispatch item = async { let! res = write item in result.Trigger res } |> dop.Throttle
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                match work.TryDequeue() with
                | true, item -> do! dispatch item
                | _ -> do! Async.Sleep 100 }

module CosmosSource =
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing

    type [<RequireQualifiedAccess; NoEquality; NoComparison>] CoordinationWork<'Pos> =
        | Result of CosmosIngester.Writer.Result
        | ProgressResult of Choice<int,exn>
        | BatchWithTracking of 'Pos * CosmosIngester.Batch[]

    let every ms f =
        let timer = Stopwatch.StartNew()
        fun () ->
            if timer.ElapsedMilliseconds > ms then
                f ()
                timer.Restart()

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
        let maxWriters = defaultArg maxWriters 64
        let statsIntervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
        let work = System.Collections.Concurrent.ConcurrentQueue()
        let buffer = CosmosIngester.Queue.StreamStates()
        let progressWriter = ProgressWriter()
        let syncState = Progress.State()
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
            let badCats = CosmosIngester.Queue.CatStats()
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
                    | CosmosIngester.Queue.Ok -> res.WriteTo writerResultLog
                    | CosmosIngester.Queue.RateLimited -> incr rateLimited
                    | CosmosIngester.Queue.Malformed -> category stream |> badCats.Ingest; incr malformed
                    | CosmosIngester.Queue.TimedOut -> incr timedOut
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
                        match buffer.TryReady() with
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
            events.Length, System.Collections.Generic.HashSet(seq { for x in events -> x.stream }).Count
        let dispose () = (coordinator :> IDisposable).Dispose()
        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let processBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : System.Collections.Generic.IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
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

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose changeLogVerbose =
        Log.Logger <-
            LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
            |> fun c -> let cfpl = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> let ol = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Information
                        c.MinimumLevel.Override(typeof<ChangeFeedObserver>.FullName, ol)
            |> fun c -> let ol = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<CosmosIngester.Writer.Result>.FullName, ol)
            |> fun c -> let cl = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<CosmosContext>.FullName, cl)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()
        Log.ForContext<ChangeFeedObserver>()

[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        let target =
            let destination = args.Destination.Connect "SyncTemplate" |> Async.RunSynchronously
            let colls = CosmosCollections(args.Destination.Database, args.Destination.Collection)
            Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.ForContext<Core.CosmosContext>())
        let log = Logging.initialize args.Verbose args.ChangeFeedVerbose
        let discovery, source, connectionPolicy, catFilter = args.Source.BuildConnectionDetails()
        let auxDiscovery, aux, leaseId, startFromHere, batchSize, lagFrequency = args.BuildChangeFeedParams()
#if marveleqx
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#else
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformOrFilter catFilter)
        // Uncomment to test marveleqx mode
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#endif
        CosmosSource.run (discovery, source) (auxDiscovery, aux) connectionPolicy
            (leaseId, startFromHere, batchSize, lagFrequency)
            createSyncHandler
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.InvalidArguments msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1