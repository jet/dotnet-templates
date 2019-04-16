module SyncTemplate.EventStoreSource

open Equinox.Store // AwaitTaskCorrect
open EventStore.ClientAPI
open System
open Serilog // NB Needs to shadow ILogger
open System.Diagnostics
open System.Threading
open System.Collections.Generic

type EventStore.ClientAPI.RecordedEvent with
    member __.Timestamp = System.DateTimeOffset.FromUnixTimeMilliseconds(__.CreatedEpoch)

let inline recPayloadBytes (x: EventStore.ClientAPI.RecordedEvent) = CosmosIngester.arrayBytes x.Data + CosmosIngester.arrayBytes x.Metadata
let inline payloadBytes (x: EventStore.ClientAPI.ResolvedEvent) = recPayloadBytes x.Event + x.OriginalStreamId.Length * 2

let tryToBatch (e : RecordedEvent) : CosmosIngester.Batch option =
    let eb = recPayloadBytes e
    if eb > CosmosIngester.cosmosPayloadLimit then
        Log.Error("ES Event Id {eventId} (#{index} in {stream}, type {type}) size {eventSize} exceeds Cosmos ingestion limit {maxCosmosBytes}",
            e.EventId, e.EventNumber, e.EventStreamId, e.EventType, eb, CosmosIngester.cosmosPayloadLimit)
        None
    else 
        let meta' = if e.Metadata <> null && e.Metadata.Length = 0 then null else e.Metadata
        let data' = if e.Data <> null && e.Data.Length = 0 then null else e.Data
        let event : Equinox.Codec.IEvent<_> = Equinox.Codec.Core.EventData.Create(e.EventType, data', meta', e.Timestamp) :> _
        Some { stream = e.EventStreamId; span = { index = e.EventNumber; events = [| event |]} }

let tryMapEvent catFilter (x : EventStore.ClientAPI.ResolvedEvent) =
    match x.Event with
    | e when not e.IsJson
        || e.EventType.StartsWith("compacted",StringComparison.OrdinalIgnoreCase)
        || e.EventStreamId.StartsWith("$") 
        || e.EventStreamId.EndsWith("_checkpoints")
        || e.EventStreamId.EndsWith("_checkpoint")
        || not (catFilter e.EventStreamId) -> None
    | e -> tryToBatch e
    
let private mb x = float x / 1024. / 1024.

let category (streamName : string) = streamName.Split([|'-'|],2).[0]

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
            Log.Information("Reader Throughput {events} events {gb:n1}GB {mbs:n2}MB/s",
                totalEvents, totalMb/1024., totalMb*1000./float overallStart.ElapsedMilliseconds)
            progressStart.Restart()

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
                        Log.Information("Reader {kind} {mb:n1}MB {events} events categories: {@cats} (MB/cat/count)", kind, mb, events, top)
                recentCats |> log "Total" 3
                recentCats |> Seq.where (fun x -> x.Key.StartsWith "$" |> not) |> log "payload" 100
                recentCats |> Seq.where (fun x -> x.Key.StartsWith "$") |> log "meta" 100
                recentCats.Clear()
                accStart.Restart()

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
        | Some max -> float __.Current.CommitPosition/float max.CommitPosition

// @scarvel8: event_global_position = 256 x 1024 x 1024 x chunk_number + chunk_header_size (128) + event_position_offset_in_chunk
let chunk (pos: Position) = uint64 pos.CommitPosition >>> 28
let posFromChunk (chunk: int) =
    let chunkBase = int64 chunk * 1024L * 1024L * 256L
    Position(chunkBase,0L)
let posFromPercentage (pct,max : Position) =
    let rawPos = Position(float max.CommitPosition * pct / 100. |> int64, 0L)
    let chunk = int (chunk rawPos) in posFromChunk chunk // &&& 0xFFFFFFFFE0000000L // rawPos / 256L / 1024L / 1024L * 1024L * 1024L * 256L

let fetchMax (conn : IEventStoreConnection) = async {
    let! lastItemBatch = conn.ReadAllEventsBackwardAsync(Position.End, 1, resolveLinkTos = false) |> Async.AwaitTaskCorrect
    let max = lastItemBatch.FromPosition
    Log.Information("EventStore Tail Position: @ {pos} ({chunks} chunks, ~{gb:n1}GB)", max.CommitPosition, chunk max, mb max.CommitPosition/1024.)
    return max }
let establishMax (conn : IEventStoreConnection) = async {
    let mutable max = None
    while Option.isNone max do
        try let! currentMax = fetchMax conn
            max <- Some currentMax
        with e ->
            Log.Warning(e,"Could not establish max position")
            do! Async.Sleep 5000 
    return Option.get max }
let pullStream (conn : IEventStoreConnection, batchSize) (stream,pos,limit : int option) (postBatch : CosmosIngester.Batch -> unit) =
    let rec fetchFrom pos limit = async {
        let reqLen = match limit with Some limit -> min limit batchSize | None -> batchSize
        let! currentSlice = conn.ReadStreamEventsForwardAsync(stream, pos, reqLen, resolveLinkTos=true) |> Async.AwaitTaskCorrect
        let events =
            [| for x in currentSlice.Events ->
                let e = x.Event
                Equinox.Codec.Core.EventData.Create(e.EventType, e.Data, e.Metadata, e.Timestamp) :> Equinox.Codec.IEvent<byte[]> |]
        postBatch { stream = stream; span = { index = currentSlice.FromEventNumber; events = events } }
        match limit with
        | None when currentSlice.IsEndOfStream -> return ()
        | None -> return! fetchFrom currentSlice.NextEventNumber None
        | Some limit when events.Length >= limit -> return ()
        | Some limit -> return! fetchFrom currentSlice.NextEventNumber (Some (limit - events.Length)) }
    fetchFrom pos limit

type [<NoComparison>] PullResult = Exn of exn: exn | Eof | EndOfTranche
let pullAll (slicesStats : SliceStatsBuffer, overallStats : OverallStats) (conn : IEventStoreConnection, batchSize)
        (range:Range, once) (tryMapEvent : ResolvedEvent -> CosmosIngester.Batch option) (postBatch : Position -> CosmosIngester.Batch[] -> unit) =
    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    let rec aux () = async {
        let! currentSlice = conn.ReadAllEventsForwardAsync(range.Current, batchSize, resolveLinkTos = false) |> Async.AwaitTaskCorrect
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let postSw = Stopwatch.StartNew()
        let batchEvents, batchBytes = slicesStats.Ingest currentSlice in overallStats.Ingest(int64 batchEvents, batchBytes)
        let batches = currentSlice.Events |> Seq.choose tryMapEvent |> Array.ofSeq
        let streams = batches |> Seq.groupBy (fun b -> b.stream) |> Array.ofSeq
        let usedStreams, usedCats = streams.Length, streams |> Seq.map fst |> Seq.distinct |> Seq.length
        postBatch currentSlice.NextPosition batches
        Log.Information("Read {pos,10} {pct:p1} {ft:n3}s {mb:n1}MB {count,4} {categories,3}c {streams,4}s {events,4}e Post {pt:n0}ms",
            range.Current.CommitPosition, range.PositionAsRangePercentage, (let e = sw.Elapsed in e.TotalSeconds), mb batchBytes,
            batchEvents, usedCats, usedStreams, batches.Length, postSw.ElapsedMilliseconds)
        if range.TryNext currentSlice.NextPosition && not once && not currentSlice.IsEndOfStream  then
            sw.Restart() // restart the clock as we hand off back to the Reader
            return! aux ()
        else
            return currentSlice.IsEndOfStream }
    async {
        try let! eof = aux ()
            return if eof then Eof else EndOfTranche
        with e -> return Exn e }

type [<NoComparison>] Work =
    | Stream of name: string * batchSize: int
    | StreamPrefix of name: string * pos: int64 * len: int * batchSize: int
    | Tranche of range: Range * batchSize : int
    | Tail of pos: Position * interval: TimeSpan * batchSize : int

type ReadQueue(batchSize, minBatchSize, ?statsInterval) =
    let work = System.Collections.Concurrent.ConcurrentQueue()
    member val OverallStats = OverallStats(?statsInterval=statsInterval)
    member val SlicesStats = SliceStatsBuffer()
    member __.QueueCount = work.Count
    member __.AddStream(name, ?batchSizeOverride) =
        work.Enqueue <| Work.Stream (name, defaultArg batchSizeOverride batchSize)
    member __.AddStreamPrefix(name, pos, len, ?batchSizeOverride) =
        work.Enqueue <| Work.StreamPrefix (name, pos, len, defaultArg batchSizeOverride batchSize)
    member __.AddTranche(range, ?batchSizeOverride) =
        work.Enqueue <| Work.Tranche (range, defaultArg batchSizeOverride batchSize)
    member __.AddTranche(pos, nextPos, max, ?batchSizeOverride) =
        __.AddTranche(Range (pos, Some nextPos, max), ?batchSizeOverride=batchSizeOverride)
    member __.AddTail(pos, interval, ?batchSizeOverride) =
        work.Enqueue <| Work.Tail (pos, interval, defaultArg batchSizeOverride batchSize)
    member __.TryDequeue () =
        work.TryDequeue()
    member __.Process(conn, tryMapEvent, postItem, shouldTail, postBatch, work) = async {
        let adjust batchSize = if batchSize > minBatchSize then batchSize - 128 else batchSize
        match work with
        | StreamPrefix (name,pos,len,batchSize) ->
            use _ = Serilog.Context.LogContext.PushProperty("Tranche",name)
            Log.Warning("Reading stream prefix; pos {pos} len {len} batch size {bs}", pos, len, batchSize)
            try do! pullStream (conn, batchSize) (name, pos, Some len) postItem
                Log.Information("completed stream prefix")
            with e ->
                let bs = adjust batchSize
                Log.Warning(e,"Could not read stream, retrying with batch size {bs}", bs)
                __.AddStreamPrefix(name, pos, len, bs)
            return false
        | Stream (name,batchSize) ->
            use _ = Serilog.Context.LogContext.PushProperty("Tranche",name)
            Log.Warning("Reading stream; batch size {bs}", batchSize)
            try do! pullStream (conn, batchSize) (name,0L,None) postItem
                Log.Information("completed stream")
            with e ->
                let bs = adjust batchSize
                Log.Warning(e,"Could not read stream, retrying with batch size {bs}", bs)
                __.AddStream(name, bs)
            return false
        | Tranche (range, batchSize) ->
            use _ = Serilog.Context.LogContext.PushProperty("Tranche",chunk range.Current)
            Log.Warning("Commencing tranche, batch size {bs}", batchSize)
            let! res = pullAll (__.SlicesStats, __.OverallStats) (conn, batchSize) (range, false) tryMapEvent postBatch 
            match res with
            | PullResult.EndOfTranche ->
                Log.Warning("Completed tranche")
                __.OverallStats.DumpIfIntervalExpired()
                return false
            | PullResult.Eof ->
                Log.Warning("REACHED THE END!")
                __.OverallStats.DumpIfIntervalExpired(true)
                return true
            | PullResult.Exn e ->
                let bs = adjust batchSize
                Log.Warning(e, "Could not read All, retrying with batch size {bs}", bs)
                __.OverallStats.DumpIfIntervalExpired()
                __.AddTranche(range, bs) 
                return false
        | Tail (pos, interval, batchSize) ->
            let mutable count, pauses, batchSize, range = 0, 0, batchSize, Range(pos, None)
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
            let mutable paused = false
            while true do
                let currentPos = range.Current
                if progressSw.ElapsedMilliseconds > progressIntervalMs then
                    Log.Information("Tailed {count} times ({pauses} waits) @ {pos} (chunk {chunk})",
                        count, pauses, currentPos.CommitPosition, chunk currentPos)
                    progressSw.Restart()
                count <- count + 1
                if shouldTail () then
                    paused <- false
                    let! res = pullAll (slicesStats,stats) (conn,batchSize) (range,true) tryMapEvent postBatch 
                    do! awaitInterval
                    match res with
                    | PullResult.EndOfTranche | PullResult.Eof -> ()
                    | PullResult.Exn e ->
                        batchSize <- adjust batchSize
                        Log.Warning(e, "Tail $all failed, adjusting batch size to {bs}", batchSize)
                else
                    if not paused then Log.Information("Pausing due to backlog of incomplete batches...")
                    paused <- true
                    pauses <- pauses + 1
                    do! awaitInterval
                stats.DumpIfIntervalExpired()
            return true } 