module SyncTemplate.EventStoreSource

open Equinox.Cosmos.Projection.Ingestion
open Equinox.Store // AwaitTaskCorrect
open Equinox.Projection
open Equinox.Projection.Engine
open EventStore.ClientAPI
open Serilog // NB Needs to shadow ILogger
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading

type EventStore.ClientAPI.RecordedEvent with
    member __.Timestamp = System.DateTimeOffset.FromUnixTimeMilliseconds(__.CreatedEpoch)

let inline recPayloadBytes (x: EventStore.ClientAPI.RecordedEvent) = State.arrayBytes x.Data + State.arrayBytes x.Metadata
let inline payloadBytes (x: EventStore.ClientAPI.ResolvedEvent) = recPayloadBytes x.Event + x.OriginalStreamId.Length * 2
let private mb x = float x / 1024. / 1024.

let toIngestionItem (e : RecordedEvent) : Engine.StreamItem =
    let meta' = if e.Metadata <> null && e.Metadata.Length = 0 then null else e.Metadata
    let data' = if e.Data <> null && e.Data.Length = 0 then null else e.Data
    let event : Equinox.Codec.IEvent<_> = Equinox.Codec.Core.EventData.Create(e.EventType, data', meta', e.Timestamp) :> _
    { stream = e.EventStreamId; index = e.EventNumber; event = event}

let category (streamName : string) = streamName.Split([|'-'|],2).[0]

/// Maintains ingestion stats (thread safe via lock free data structures so it can be used across multiple overlapping readers)
type OverallStats(?statsInterval) =
    let intervalMs = let t = defaultArg statsInterval (TimeSpan.FromMinutes 5.) in t.TotalMilliseconds |> int64
    let overallStart, progressStart = Stopwatch.StartNew(), Stopwatch.StartNew()
    let mutable totalEvents, totalBytes = 0L, 0L
    member __.Ingest(batchEvents, batchBytes) = 
        Interlocked.Add(&totalEvents,batchEvents) |> ignore
        Interlocked.Add(&totalBytes,batchBytes) |> ignore
    member __.Bytes = totalBytes
    member __.Events = totalEvents
    member __.DumpIfIntervalExpired(?force) =
        if progressStart.ElapsedMilliseconds > intervalMs || force = Some true then
            let totalMb = mb totalBytes
            if totalEvents <> 0L then
                Log.Information("Reader Throughput {events} events {gb:n1}GB {mb:n2}MB/s",
                    totalEvents, totalMb/1024., totalMb*1000./float overallStart.ElapsedMilliseconds)
            progressStart.Restart()

/// Maintains stats for traversals of $all; Threadsafe [via naive locks] so can be used by multiple stripes reading concurrently
type SliceStatsBuffer(?interval) =
    let intervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 5.) in t.TotalMilliseconds |> int64
    let recentCats, accStart = Dictionary<string,int*int>(), Stopwatch.StartNew()
    member __.Ingest(slice: AllEventsSlice) =
        lock recentCats <| fun () ->
            let mutable batchBytes = 0
            for x in slice.Events do
                let cat = category x.OriginalStreamId
                let eventBytes = payloadBytes x
                match recentCats.TryGetValue cat with
                | true, (currCount, currSize) -> recentCats.[cat] <- (currCount + 1, currSize+eventBytes)
                | false, _ -> recentCats.[cat] <- (1, eventBytes)
                batchBytes <- batchBytes + eventBytes
            __.DumpIfIntervalExpired()
            slice.Events.Length, int64 batchBytes
    member __.DumpIfIntervalExpired(?force) =
        if accStart.ElapsedMilliseconds > intervalMs || defaultArg force false then
            lock recentCats <| fun () ->
                let log kind limit xs =
                    let cats =
                        [| for KeyValue (s,(c,b)) in xs |> Seq.sortByDescending (fun (KeyValue (_,(_,b))) -> b) ->
                            mb (int64 b) |> round, s, c |]
                    if (not << Array.isEmpty) cats then
                        let mb, events, top = Array.sumBy (fun (mb, _, _) -> mb) cats, Array.sumBy (fun (_, _, c) -> c) cats, Seq.truncate limit cats
                        Log.Information("Reader {kind} {mb:n0}MB {events:n0} events categories: {@cats} (MB/cat/count)", kind, mb, events, top)
                recentCats |> log "Total" 3
                recentCats |> Seq.where (fun x -> x.Key.StartsWith "$" |> not) |> log "payload" 100
                recentCats |> Seq.where (fun x -> x.Key.StartsWith "$") |> log "meta" 100
                recentCats.Clear()
                accStart.Restart()

/// Defines a tranche of a traversal of a stream (or the store as a whole)
type Range(start, sliceEnd : Position option, ?max : Position) =
    member val Current = start with get, set
    member __.TryNext(pos: Position) =
        __.Current <- pos
        __.IsCompleted
    member __.IsCompleted =
        match sliceEnd with
        | Some send when __.Current.CommitPosition >= send.CommitPosition -> false
        | _ -> true
    member __.PositionAsRangePercentage =
        match max with
        | None -> Double.NaN
        | Some max ->
            match __.Current.CommitPosition, max.CommitPosition with
            | p,m when p > m -> Double.NaN
            | p,m -> float p / float m

(* Logic for computation of chunk offsets; ES writes chunks whose index starts at a multiple of 256MB
   to be able to address an arbitrary position as a percentage, we need to consider this aspect as only a valid Position can be supplied to the read call *)

// @scarvel8: event_global_position = 256 x 1024 x 1024 x chunk_number + chunk_header_size (128) + event_position_offset_in_chunk
let chunk (pos: Position) = uint64 pos.CommitPosition >>> 28
let posFromChunk (chunk: int) =
    let chunkBase = int64 chunk * 1024L * 1024L * 256L
    Position(chunkBase,0L)
let posFromChunkAfter (pos: EventStore.ClientAPI.Position) =
    let nextChunk = 1 + int (chunk pos)
    posFromChunk nextChunk
let posFromPercentage (pct,max : Position) =
    let rawPos = Position(float max.CommitPosition * pct / 100. |> int64, 0L)
    let chunk = int (chunk rawPos) in posFromChunk chunk // &&& 0xFFFFFFFFE0000000L // rawPos / 256L / 1024L / 1024L * 1024L * 1024L * 256L

/// Read the current tail position; used to be able to compute and log progress of ingestion
let fetchMax (conn : IEventStoreConnection) = async {
    let! lastItemBatch = conn.ReadAllEventsBackwardAsync(Position.End, 1, resolveLinkTos = false) |> Async.AwaitTaskCorrect
    let max = lastItemBatch.FromPosition
    Log.Information("EventStore Tail Position: @ {pos} ({chunks} chunks, ~{gb:n1}GB)", max.CommitPosition, chunk max, mb max.CommitPosition/1024.)
    return max }
/// `fetchMax` wrapped in a retry loop; Sync process is entirely reliant on establishing the max so we have a crude retry loop
let establishMax (conn : IEventStoreConnection) = async {
    let mutable max = None
    while Option.isNone max do
        try let! currentMax = fetchMax conn
            max <- Some currentMax
        with e ->
            Log.Warning(e,"Could not establish max position")
            do! Async.Sleep 5000 
    return Option.get max }

/// Walks a stream within the specified constraints; used to grab data when writing to a stream for which a prefix is missing
/// Can throw (in which case the caller is in charge of retrying, possibly with a smaller batch size)
let pullStream (conn : IEventStoreConnection, batchSize) (stream,pos,limit : int option) (postBatch : State.StreamSpan -> Async<unit>) =
    let rec fetchFrom pos limit = async {
        let reqLen = match limit with Some limit -> min limit batchSize | None -> batchSize
        let! currentSlice = conn.ReadStreamEventsForwardAsync(stream, pos, reqLen, resolveLinkTos=true) |> Async.AwaitTaskCorrect
        let events =
            [| for x in currentSlice.Events ->
                let e = x.Event
                Equinox.Codec.Core.EventData.Create(e.EventType, e.Data, e.Metadata, e.Timestamp) :> Equinox.Codec.IEvent<byte[]> |]
        do! postBatch { stream = stream; span = { index = currentSlice.FromEventNumber; events = events } }
        match limit with
        | None when currentSlice.IsEndOfStream -> return ()
        | None -> return! fetchFrom currentSlice.NextEventNumber None
        | Some limit when events.Length >= limit -> return ()
        | Some limit -> return! fetchFrom currentSlice.NextEventNumber (Some (limit - events.Length)) }
    fetchFrom pos limit

/// Walks the $all stream, yielding batches together with the associated Position info for the purposes of checkpointing
/// Can throw (in which case the caller is in charge of retrying, possibly with a smaller batch size)
type [<NoComparison>] PullResult = Exn of exn: exn | Eof | EndOfTranche
let pullAll (slicesStats : SliceStatsBuffer, overallStats : OverallStats) (conn : IEventStoreConnection, batchSize)
        (range:Range, once) (tryMapEvent : ResolvedEvent -> Engine.StreamItem option) (postBatch : Position -> Engine.StreamItem[] -> Async<int*int>) =
    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    let rec aux () = async {
        let! currentSlice = conn.ReadAllEventsForwardAsync(range.Current, batchSize, resolveLinkTos = false) |> Async.AwaitTaskCorrect
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let postSw = Stopwatch.StartNew()
        let batchEvents, batchBytes = slicesStats.Ingest currentSlice in overallStats.Ingest(int64 batchEvents, batchBytes)
        let batches = currentSlice.Events |> Seq.choose tryMapEvent |> Array.ofSeq
        let streams = batches |> Seq.groupBy (fun b -> b.stream) |> Array.ofSeq
        let usedStreams, usedCats = streams.Length, streams |> Seq.map fst |> Seq.distinct |> Seq.length
        let! (cur,max) = postBatch currentSlice.NextPosition batches
        Log.Information("Read {pos,10} {pct:p1} {ft:n3}s {mb:n1}MB {count,4} {categories,4}c {streams,4}s {events,4}e Post {pt:n3}s {cur}/{max}",
            range.Current.CommitPosition, range.PositionAsRangePercentage, (let e = sw.Elapsed in e.TotalSeconds), mb batchBytes,
            batchEvents, usedCats, usedStreams, batches.Length, (let e = postSw.Elapsed in e.TotalSeconds), cur, max)
        if not (range.TryNext currentSlice.NextPosition && not once && not currentSlice.IsEndOfStream) then
            return currentSlice.IsEndOfStream
        else
            sw.Restart() // restart the clock as we hand off back to the Reader
            return! aux () }
    async {
        try let! eof = aux ()
            return if eof then Eof else EndOfTranche
        with e -> return Exn e }

/// Specification for work to be performed by a reader thread
[<NoComparison>]
type ReadRequest =
    /// Tail from a given start position, at intervals of the specified timespan (no waiting if catching up)
    | Tail of pos: Position * max : Position * interval: TimeSpan * batchSize : int
    /// Read a given segment of a stream (used when a stream needs to be rolled forward to lay down an event for which the preceding events are missing)
    | StreamPrefix of name: string * pos: int64 * len: int * batchSize: int
    /// Read the entirity of a stream in blocks of the specified batchSize (TODO wire to commandline request)
    | Stream of name: string * batchSize: int
    /// Read a specific chunk (min-max range), posting batches tagged with that chunk number
    | Chunk of chunk: int * range: Range * batchSize : int

/// Data with context resulting from a reader thread
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type ReadResult =
    /// A batch read from a Chunk
    | ChunkBatch of chunk: int * pos: Position * items: StreamItem seq
    /// Ingestion buffer requires an explicit end of chunk message before next chunk can commence processing
    | EndOfChunk of chunk: int
    /// A Batch read from a Stream or StreamPrefix
    | StreamSpan of span: State.StreamSpan
    /// A batch read by `Tail`
    | Batch of pos: Position * items: StreamItem seq

/// Holds work queue, together with stats relating to the amount and/or categories of data being traversed
/// Processing is driven by external callers running multiple concurrent invocations of `Process`
type ReadQueue(batchSize, minBatchSize, ?statsInterval) =
    let work = System.Collections.Concurrent.ConcurrentQueue()
    member val OverallStats = OverallStats(?statsInterval=statsInterval)
    member val SlicesStats = SliceStatsBuffer()
    member __.DefaultBatchSize = batchSize
    member __.QueueCount = work.Count
    member __.AddStream(name, ?batchSizeOverride) =
        work.Enqueue <| ReadRequest.Stream (name, defaultArg batchSizeOverride batchSize)
    member __.AddStreamPrefix(name, pos, len, ?batchSizeOverride) =
        work.Enqueue <| ReadRequest.StreamPrefix (name, pos, len, defaultArg batchSizeOverride batchSize)
    member private __.AddTranche(chunk, range, ?batchSizeOverride) =
        work.Enqueue <| ReadRequest.Chunk (chunk, range, defaultArg batchSizeOverride batchSize)
    member __.AddTranche(chunk, pos, nextPos, max, ?batchSizeOverride) =
        __.AddTranche(chunk, Range (pos, Some nextPos, max), ?batchSizeOverride=batchSizeOverride)
    member __.AddTail(pos, max, interval, ?batchSizeOverride) =
        work.Enqueue <| ReadRequest.Tail (pos, max, interval, defaultArg batchSizeOverride batchSize)
    member __.TryDequeue () =
        work.TryDequeue()
    /// Invoked by Dispatcher to process a tranche of work; can have parallel invocations
    member __.Process(conn, tryMapEvent, post : ReadResult -> Async<int*int>, work) = async {
        let adjust batchSize = if batchSize > minBatchSize then batchSize - 128 else batchSize
        let postSpan = ReadResult.StreamSpan >> post >> Async.Ignore
        match work with
        | StreamPrefix (name,pos,len,batchSize) ->
            use _ = Serilog.Context.LogContext.PushProperty("Tranche",name)
            Log.Warning("Reading stream prefix; pos {pos} len {len} batch size {bs}", pos, len, batchSize)
            try let! t,() = pullStream (conn, batchSize) (name, pos, Some len) postSpan |> Stopwatch.Time
                Log.Information("completed stream prefix in {ms:n3}s", let e = t.Elapsed in e.TotalSeconds)
            with e ->
                let bs = adjust batchSize
                Log.Warning(e,"Could not read stream, retrying with batch size {bs}", bs)
                __.AddStreamPrefix(name, pos, len, bs)
            return false
        | Stream (name,batchSize) ->
            use _ = Serilog.Context.LogContext.PushProperty("Tranche",name)
            Log.Warning("Reading stream; batch size {bs}", batchSize)
            try let! t,() = pullStream (conn, batchSize) (name,0L,None) postSpan |> Stopwatch.Time
                Log.Information("completed stream in {ms:n3}s", let e = t.Elapsed in e.TotalSeconds)
            with e ->
                let bs = adjust batchSize
                Log.Warning(e,"Could not read stream, retrying with batch size {bs}", bs)
                __.AddStream(name, bs)
            return false
        | Chunk (chunk, range, batchSize) ->
            use _ = Serilog.Context.LogContext.PushProperty("Tranche", chunk)
            Log.Warning("Commencing tranche, batch size {bs}", batchSize)
            let postBatch pos items = post (ReadResult.ChunkBatch (chunk, pos, items))
            let! t, res = pullAll (__.SlicesStats, __.OverallStats) (conn, batchSize) (range, false) tryMapEvent postBatch |> Stopwatch.Time
            match res with
            | PullResult.EndOfTranche ->
                Log.Warning("completed tranche in {ms:n3}m", let e = t.Elapsed in e.TotalMinutes)
                __.OverallStats.DumpIfIntervalExpired()
                let! _ = post (ReadResult.EndOfChunk chunk)
                return false
            | PullResult.Eof ->
                Log.Warning("completed tranche AND REACHED THE END in {ms:n3}m", let e = t.Elapsed in e.TotalMinutes)
                __.OverallStats.DumpIfIntervalExpired(true)
                let! _ = post (ReadResult.EndOfChunk chunk)
                return true
            | PullResult.Exn e ->
                let bs = adjust batchSize
                Log.Warning(e, "Could not read All, retrying with batch size {bs}", bs)
                __.OverallStats.DumpIfIntervalExpired()
                __.AddTranche(chunk, range, bs) 
                return false
        | Tail (pos, max, interval, batchSize) ->
            let mutable count, batchSize, range = 0, batchSize, Range(pos, None, max)
            let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
            let progressIntervalMs, tailIntervalMs = int64 statsInterval.TotalMilliseconds, int64 interval.TotalMilliseconds
            let tailSw = Stopwatch.StartNew()
            let awaitInterval = async {
                match tailIntervalMs - tailSw.ElapsedMilliseconds with
                | waitTimeMs when waitTimeMs > 0L -> do! Async.Sleep (int waitTimeMs)
                | _ -> ()
                tailSw.Restart() }
            let slicesStats, stats = SliceStatsBuffer(), OverallStats()
            use _ = Serilog.Context.LogContext.PushProperty("Tranche", "Tail")
            let progressSw = Stopwatch.StartNew()
            let postBatch pos items = post (ReadResult.Batch (pos, items))
            while true do
                let currentPos = range.Current
                if progressSw.ElapsedMilliseconds > progressIntervalMs then
                    Log.Information("Tailed {count} times @ {pos} (chunk {chunk})",
                        count, currentPos.CommitPosition, chunk currentPos)
                    progressSw.Restart()
                count <- count + 1
                let! res = pullAll (slicesStats,stats) (conn,batchSize) (range,true) tryMapEvent postBatch
                do! awaitInterval
                match res with
                | PullResult.EndOfTranche | PullResult.Eof -> ()
                | PullResult.Exn e ->
                    batchSize <- adjust batchSize
                    Log.Warning(e, "Tail $all failed, adjusting batch size to {bs}", batchSize)
                stats.DumpIfIntervalExpired()
            return true } 

/// Handles bulk ingestion - a specified number of concurrent reader threads round-robin over a supplied set of connections, taking the next available 256MB
/// chunk from the tail upon completion of the specified `Range` delimiting the chunk
type StripedReader(conns : _ array, work: ReadQueue, tryMapEvent: EventStore.ClientAPI.ResolvedEvent -> StreamItem option, maxDop) = 
    let dop = new SemaphoreSlim(maxDop)

    /// Single invocation; spawns child threads within defined limits; exits when End of Store has been reached
    member __.Pump(post, startPos, max) = async {
        let! ct = Async.CancellationToken
        let mutable remainder =
            let nextPos = posFromChunkAfter startPos
            let startChunk = chunk startPos |> int
            work.AddTranche(startChunk, startPos, nextPos, max)
            Some nextPos
        let mutable finished = false
        let r = new Random()
        let mutable robin = 0
        while not ct.IsCancellationRequested && not (finished && dop.CurrentCount <> maxDop) do
            work.OverallStats.DumpIfIntervalExpired()
            let! _ = dop.Await()
            let forkRunRelease task = async {
                let! _ = Async.StartChild <| async {
                    try let connIndex = Interlocked.Increment(&robin) % conns.Length
                        let conn = conns.[connIndex]
                        let! eof = work.Process(conn, tryMapEvent, post, task) in ()
                        if eof then remainder <- None
                    finally dop.Release() |> ignore }
                return () }
            let currentCount = dop.CurrentCount
            // Jitter is most relevant when processing commences - any commencement of a chunk can trigger significant page faults on server
            // which we want to attempt to limit the effects of
            let jitter = match currentCount with 0 -> 200 | x -> r.Next(1000, 2000)
            Log.Warning("Waiting {jitter}ms to jitter reader stripes, {currentCount} further reader stripes awaiting start", jitter, currentCount)
            do! Async.Sleep jitter
            match work.TryDequeue() with
            | true, task ->
                do! forkRunRelease task
            | false, _ ->
                match remainder with
                | Some pos -> 
                    let nextPos = posFromChunkAfter pos
                    remainder <- Some nextPos
                    let chunkNumber = chunk pos |> int
                    do! forkRunRelease <| ReadRequest.Chunk (chunkNumber, Range(pos, Some nextPos, max), work.DefaultBatchSize)
                | None ->
                    Log.Warning("No further ingestion work to commence, transitioning to tailing...")
                    finished <- true }

/// Handles Tailing mode - a single reader thread together with a (limited) set of concurrent of stream-catchup readers
type TailAndPrefixesReader(conn, work: ReadQueue, tryMapEvent: EventStore.ClientAPI.ResolvedEvent -> StreamItem option, maxCatchupReaders) = 
    // to avoid busy waiting in main message pummp loop
    let sleepIntervalMs = 100
    let dop = new SemaphoreSlim(1 + maxCatchupReaders)
    member __.HasCapacity = work.QueueCount < dop.CurrentCount
    // TODO reinstate usage
    member __.AddStreamPrefix(stream, pos, len) = work.AddStreamPrefix(stream, pos, len)

    /// Single invcation will run until Cancelled, spawning child threads as necessary
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

type StartMode = Starting | Resuming | Overridding

let run (log : Serilog.ILogger) (connect, spec, tryMapEvent) maxReadAhead maxProcessing (cosmosContext, maxWriters) resolveCheckpointStream = async {
    let checkpoints = Checkpoint.CheckpointSeries(spec.groupName, log.ForContext<Checkpoint.CheckpointSeries>(), resolveCheckpointStream)
    let conn = connect ()
    let! maxInParallel = Async.StartChild <| establishMax conn
    let! initialCheckpointState = checkpoints.Read
    let! max = maxInParallel
    let! startPos = async {
        let mkPos x = EventStore.ClientAPI.Position(x, 0L)
        let requestedStartPos =
            match spec.start with
            | Absolute p -> mkPos p
            | Chunk c -> posFromChunk c
            | Percentage pct -> posFromPercentage (pct, max)
            | TailOrCheckpoint -> max
            | StartOrCheckpoint -> EventStore.ClientAPI.Position.Start
        let! startMode, startPos, checkpointFreq = async {
            match initialCheckpointState, requestedStartPos with
            | Checkpoint.Folds.NotStarted, r ->
                if spec.forceRestart then invalidOp "Cannot specify --forceRestart when no progress yet committed"
                do! checkpoints.Start(spec.checkpointInterval, r.CommitPosition)
                return Starting, r, spec.checkpointInterval
            | Checkpoint.Folds.Running s, _ when not spec.forceRestart ->
                return Resuming, mkPos s.state.pos, TimeSpan.FromSeconds(float s.config.checkpointFreqS)
            | Checkpoint.Folds.Running _, r ->
                do! checkpoints.Override(spec.checkpointInterval, r.CommitPosition)
                return Overridding, r, spec.checkpointInterval
        }
        log.Information("Sync {mode} {groupName} @ {pos} (chunk {chunk}, {pct:p1}) checkpointing every {checkpointFreq:n1}m",
            startMode, spec.groupName, startPos.CommitPosition, chunk startPos, float startPos.CommitPosition/float max.CommitPosition,
            checkpointFreq.TotalMinutes)
        return startPos }
    let ingestionEngine = startIngestionEngine (log, maxProcessing, cosmosContext, maxWriters, TimeSpan.FromMinutes 1.)
    let trancheEngine = TrancheEngine.Start (log, ingestionEngine, maxReadAhead, maxProcessing, TimeSpan.FromMinutes 1.)
    let queue = ReadQueue(spec.batchSize, spec.minBatchSize)
    if spec.gorge then
        let extraConns = Seq.init (spec.stripes-1) (ignore >> connect)
        let conns = [| yield conn; yield! extraConns |]
        let post = function
            | ReadResult.ChunkBatch (chunk, pos, xs) ->
                let cp = pos.CommitPosition
                trancheEngine.Submit <| Push.ChunkBatch(chunk, cp, checkpoints.Commit cp, xs)
            | ReadResult.EndOfChunk chunk ->
                trancheEngine.Submit <| Push.EndOfChunk chunk
            | ReadResult.Batch _
            | ReadResult.StreamSpan _ as x ->
                failwithf "%A not supported when gorging" x
        let startChunk = chunk startPos |> int
        let! _ = trancheEngine.Submit (Push.SetActiveChunk startChunk)
        log.Information("Gorging with {stripes} $all reader stripes covering a 256MB chunk each", spec.stripes)
        let gorgingReader = StripedReader(conns, queue, tryMapEvent, spec.stripes)
        do! gorgingReader.Pump(post, startPos, max)
        for x in extraConns do x.Close()
    // After doing the gorging, we switch to normal tailing (which avoids the app exiting too)
    let post = function
        | ReadResult.Batch (pos, xs) ->
            let cp = pos.CommitPosition
            trancheEngine.Submit(cp, checkpoints.Commit cp, xs)
        | ReadResult.StreamSpan span ->
            trancheEngine.Submit <| Push.Stream span
        | ReadResult.ChunkBatch _
        | ReadResult.EndOfChunk _ as x ->
            failwithf "%A not supported when tailing" x
    let readers = TailAndPrefixesReader(conn, queue, tryMapEvent, spec.stripes)
    log.Information("Tailing every every {intervalS:n1}s TODO with {streamReaders} stream catchup-readers", spec.tailInterval.TotalSeconds, spec.stripes)
    do! readers.Pump(post, startPos, max, spec.tailInterval) }