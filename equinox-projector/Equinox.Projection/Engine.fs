module Equinox.Projection.Engine

open Equinox.Projection.State
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

type [<NoComparison>] StreamItem = { stream: string; index: int64; event: Equinox.Codec.IEvent<byte[]> }

[<NoComparison; NoEquality>]
type Message<'R> =
    /// Enqueue a batch of items with supplied progress marking function
    | Add of markCompleted: (unit -> unit) * items: StreamItem[]
    | AddStream of StreamSpan
    | AddActive of KeyValuePair<string,StreamState>[]
    /// Feed stats about an ingested batch to relevant listeners
    | Added of streams: int * skip: int * events: int
    /// Result of processing on stream - result (with basic stats) or the `exn` encountered
    | Result of stream: string * outcome: Choice<'R,exn>
    
type Stats<'R>(log : ILogger, statsInterval : TimeSpan) =
    let cycles, batchesPended, streamsPended, eventsSkipped, eventsPended, resultCompleted, resultExn = ref 0, ref 0, ref 0, ref 0, ref 0, ref 0, ref 0
    let statsDue = expiredMs (int64 statsInterval.TotalMilliseconds)
    let dumpStats (available,maxDop) =
        log.Information("Projection Cycles {cycles} Active {busy}/{processors} Ingested {batches} ({streams:n0}s {events:n0}-{skipped:n0}e) Completed {completed} Exceptions {exns}",
            !cycles, maxDop-available, maxDop, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped, !resultCompleted, !resultExn)
        cycles := 0; batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0; resultCompleted := 0; resultExn:= 0
    abstract member Handle : Message<'R> -> unit
    default __.Handle res =
        match res with
        | Add _ | AddStream _ | AddActive _ -> ()
        | Added (streams, skipped, events) ->
            incr batchesPended
            streamsPended := !streamsPended + streams
            eventsPended := !eventsPended + events
            eventsSkipped := !eventsSkipped + skipped
        | Result (_stream, Choice1Of2 _) ->
            incr resultCompleted
        | Result (_stream, Choice2Of2 _) ->
            incr resultExn
    member __.TryDump((available,maxDop),streams : StreamStates) =
        incr cycles
        if statsDue () then
            dumpStats (available,maxDop)
            __.DumpExtraStats()
            streams.Dump log
    abstract DumpExtraStats : unit -> unit
    default __.DumpExtraStats () = ()

/// Consolidates ingested events into streams; coordinates dispatching of these in priority dictated by the needs of the checkpointing approach in force
/// a) does not itself perform any readin activities
/// b) manages writing of progress
/// x) periodically reports state (with hooks for ingestion engines to report same)
type ProjectionEngine<'R>(maxPendingBatches, dispatcher : Dispatcher<_>, project : int64 option * StreamSpan -> Async<string * Choice<'R,exn>>, handleResult) =
    let sleepIntervalMs = 1
    let cts = new CancellationTokenSource()
    let batches = new SemaphoreSlim(maxPendingBatches)
    let work = ConcurrentQueue<Message<'R>>()
    let streams = StreamStates()
    let progressState = ProgressState()

    member private __.Pump(stats : Stats<'R>) = async {
        use _ = dispatcher.Result.Subscribe(Result >> work.Enqueue)
        Async.Start(dispatcher.Pump(), cts.Token)
        let validVsSkip (streamState : StreamState) (item : StreamItem) =
            match streamState.write, item.index + 1L with
            | Some cw, required when cw >= required -> 0, 1
            | _ -> 1, 0
        let handle x =
            match x with
            | Add (checkpoint, items) ->
                let reqs = Dictionary()
                let mutable count, skipCount = 0, 0
                for item in items do
                    let stream,streamState = streams.Add(item.stream,item.index,item.event)
                    match validVsSkip streamState item with
                    | 0, skip ->
                        skipCount <- skipCount + skip
                    | required, _ ->
                        count <- count + required
                        reqs.[stream] <- item.index+1L
                progressState.AppendBatch(checkpoint,reqs)
                work.Enqueue(Added (reqs.Count,skipCount,count))
            | AddActive events ->
                for e in events do
                    streams.InternalMerge(e.Key,e.Value)
            | AddStream streamSpan ->
                let _stream,_streamState = streams.Add(streamSpan,false)
                work.Enqueue(Added (1,0,streamSpan.span.events.Length)) // Yes, need to compute skip
            | Added _  ->
                ()
            | Result _ as r ->
                handleResult (streams, progressState, batches) r
            
        while not cts.IsCancellationRequested do
            // 1. propagate read items to buffer; propagate write write results to buffer and progress write impacts to local state
            work |> ConcurrentQueue.drain (fun x -> handle x; stats.Handle x)
            // 2. Mark off any progress achieved (releasing memory and/or or unblocking reading of batches)
            let completedBatches = progressState.Validate(streams.TryGetStreamWritePos)
            if completedBatches > 0 then batches.Release(completedBatches) |> ignore
            // 3. After that, provision writers queue
            let capacity,_ = dispatcher.AvailableCapacity
            if capacity <> 0 then
                let work = streams.Schedule(progressState.ScheduledOrder streams.QueueLength, capacity)
                let xs = (Seq.ofArray work).GetEnumerator()
                let mutable ok = true
                while xs.MoveNext() && ok do
                    let! succeeded = dispatcher.TryAdd(project xs.Current)
                    ok <- succeeded
            // 4. Periodically emit status info
            stats.TryDump(dispatcher.AvailableCapacity,streams)
            do! Async.Sleep sleepIntervalMs }
    static member Start<'R>(stats, maxPendingBatches, processorDop, project, handleResult) =
        let dispatcher = Dispatcher(processorDop)
        let instance = new ProjectionEngine<'R>(maxPendingBatches, dispatcher, project, handleResult)
        Async.Start <| instance.Pump(stats)
        instance

    member __.TrySubmit(markCompleted, events) = async {
        let! got = batches.Await(TimeSpan.Zero)
        if got then work.Enqueue <| Add (markCompleted, events); return true
        else return false }

    member __.Submit(markCompleted, events) = async {
        let! _ = batches.Await()
        work.Enqueue <| Add (markCompleted, Array.ofSeq events)
        return maxPendingBatches-batches.CurrentCount,maxPendingBatches }

    member __.AllStreams = streams.All

    member __.AddActiveStreams(events) =
        work.Enqueue <| AddActive events

    member __.Submit(streamSpan) =
        work.Enqueue <| AddStream streamSpan

    member __.Stop() =
        cts.Cancel()

let startProjectionEngine (log, maxPendingBatches, processorDop, project : StreamSpan -> Async<int>, statsInterval) =
    let project (_maybeWritePos, batch) = async {
        try let! count = project batch
            return batch.stream, Choice1Of2 (batch.span.index + int64 count)
        with e -> return batch.stream, Choice2Of2 e }
    let handleResult (streams: StreamStates, progressState : ProgressState<_>,  batches: SemaphoreSlim) = function 
        | Result (stream, Choice1Of2 index) ->
            match progressState.MarkStreamProgress(stream,index) with 0 -> () | batchesCompleted -> batches.Release(batchesCompleted) |> ignore
            streams.MarkCompleted(stream,index)
        | Result (stream, Choice2Of2 _) ->
            streams.MarkFailed stream
        | _ -> ()
    let stats = Stats(log, statsInterval)
    ProjectionEngine<int64>.Start(stats, maxPendingBatches, processorDop, project, handleResult)
    
type Sem(max) =
    let inner = new SemaphoreSlim(max)
    member __.Release(?count) = match defaultArg count 1 with 0 -> () | x -> inner.Release x |> ignore
    member __.State = max-inner.CurrentCount,max 
    member __.Await() = inner.Await() |> Async.Ignore
    member __.HasCapacity = inner.CurrentCount > 0
    member __.TryAwait(?timeout) = inner.Await(?timeout=timeout)

type TrancheStreamBuffer() =
    let states = Dictionary<string, StreamState>()
    let merge stream (state : StreamState) =
        match states.TryGetValue stream with
        | false, _ ->
            states.Add(stream, state)
        | true, current ->
            let updated = StreamState.combine current state
            states.[stream] <- updated

    member __.Merge(items : StreamItem seq) =
        for item in items do
            merge item.stream { isMalformed = false; write = None; queue = [| { index = item.index; events = Array.singleton item.event } |] }

    member __.Take(processingContains) =
        let forward = [| for x in states do if processingContains x.Key then yield x |]
        for x in forward do states.Remove x.Key |> ignore
        forward

    member __.Dump(log : ILogger) =
        let mutable waiting, waitingB = 0, 0L
        let waitingCats, waitingStreams = CatStats(), CatStats()
        for KeyValue (stream,state) in states do
            let sz = int64 state.Size
            waitingCats.Ingest(category stream)
            waitingStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.Length, (sz + 512L) / 1024L)
            waiting <- waiting + 1
            waitingB <- waitingB + sz
        log.Information("Streams Waiting {busy:n0}/{busyMb:n1}MB ", waiting, mb waitingB)
        if waitingCats.Any then log.Information("Waiting Categories, events {readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
        if waitingCats.Any then log.Information("Waiting Streams, KB {readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

/// Manages writing of progress
/// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
/// - retries until success or a new item is posted
type ProgressWriter<'Res when 'Res: equality>() =
    let pumpSleepMs = 100
    let due = expiredMs 5000L
    let mutable committedEpoch = None
    let mutable validatedPos = None
    let result = Event<Choice<'Res,exn>>()
    [<CLIEvent>] member __.Result = result.Publish
    member __.Post(version,f) =
        Volatile.Write(&validatedPos,Some (version,f))
    member __.CommittedEpoch = Volatile.Read(&committedEpoch)
    member __.Pump() = async {
        let! ct = Async.CancellationToken
        while not ct.IsCancellationRequested do
            match Volatile.Read &validatedPos with
            | Some (v,f) when Volatile.Read(&committedEpoch) <> Some v && due () ->
                try do! f
                    Volatile.Write(&committedEpoch, Some v)
                    result.Trigger (Choice1Of2 v)
                with e -> result.Trigger (Choice2Of2 e)
            | _ -> do! Async.Sleep pumpSleepMs }
            
[<NoComparison; NoEquality>]
type SeriesMessage =
    | Add of epoch: int64 * markCompleted: Async<unit> * items: StreamItem seq
    | Added of streams: int * events: int
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Choice<int64,exn>

type TrancheStats(log : ILogger, maxPendingBatches, statsInterval : TimeSpan) =
    let mutable pendingBatchCount, validatedEpoch, comittedEpoch : int * int64 option * int64 option = 0, None, None
    let progCommitFails, progCommits = ref 0, ref 0 
    let cycles, batchesPended, streamsPended, eventsPended = ref 0, ref 0, ref 0, ref 0
    let statsDue = expiredMs (int64 statsInterval.TotalMilliseconds)
    let dumpStats (available,maxDop) =
        log.Information("Tranche Cycles {cycles} Active {active}/{writers} Batches {batches} ({streams:n0}s {events:n0}e)",
            !cycles, available, maxDop, !batchesPended, !streamsPended, !eventsPended)
        cycles := 0; batchesPended := 0; streamsPended := 0; eventsPended := 0
        if !progCommitFails <> 0 || !progCommits <> 0 then
            match comittedEpoch with
            | None ->
                log.Error("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated}; writing failing: {failures} failures ({commits} successful commits)",
                        pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, !progCommitFails, !progCommits)
            | Some committed when !progCommitFails <> 0 ->
                log.Warning("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated} (committed: {committed}, {commits} commits, {failures} failures)",
                        pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, committed, !progCommits, !progCommitFails)
            | Some committed ->
                log.Information("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated} (committed: {committed}, {commits} commits)",
                        pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, committed, !progCommits)
            progCommits := 0; progCommitFails := 0
        else
            log.Information("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated} (committed: {committed})",
                    pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, Option.toNullable comittedEpoch)
    member __.Handle : SeriesMessage -> unit = function
        | Add _ -> () // Enqueuing of an event is not interesting - we assume it'll get processed and mapped to an `Added` in the same cycle
        | ProgressResult (Choice1Of2 epoch) ->
            incr progCommits
            comittedEpoch <- Some epoch
        | ProgressResult (Choice2Of2 (_exn : exn)) ->
            incr progCommitFails
        | Added (streams,events) ->
            incr batchesPended
            streamsPended := !streamsPended + streams
            eventsPended := !eventsPended + events
    member __.HandleValidated(epoch, pendingBatches) = 
        validatedEpoch <- epoch
        pendingBatchCount <- pendingBatches
    member __.HandleCommitted epoch = 
        comittedEpoch <- epoch
    member __.TryDump((available,maxDop),streams : TrancheStreamBuffer) =
        incr cycles
        if statsDue () then
            dumpStats (available,maxDop)
            streams.Dump log

/// Holds batches away from Core processing to limit in-flight processsing
type TrancheEngine<'R>(log : ILogger, ingester: ProjectionEngine<'R>, maxQueued, maxSubmissions, statsInterval : TimeSpan, ?pumpDelayMs) =
    let cts = new CancellationTokenSource()
    let work = ConcurrentQueue<SeriesMessage>()
    let read = new Sem(maxQueued)
    let write = new Sem(maxSubmissions)
    let streams = TrancheStreamBuffer()
    let pending = Queue<_>()
    let mutable validatedPos = None
    let progressWriter = ProgressWriter<_>()
    let stats = TrancheStats(log, maxQueued, statsInterval)
    let pumpDelayMs = defaultArg pumpDelayMs 5

    member private __.Pump() = async {
        let handle = function
            | Add (epoch, checkpoint, items) ->
                let items = Array.ofSeq items
                streams.Merge items
                let markCompleted () =
                    write.Release()
                    read.Release()
                    validatedPos <- Some (epoch,checkpoint)
                work.Enqueue(Added (HashSet(seq { for x in items -> x.stream }).Count,items.Length))
                pending.Enqueue((markCompleted,items))
            | Added _ | ProgressResult _ -> ()
        use _ = progressWriter.Result.Subscribe(ProgressResult >> work.Enqueue)
        Async.Start(progressWriter.Pump(), cts.Token)
        while not cts.IsCancellationRequested do
            work |> ConcurrentQueue.drain (fun x -> handle x; stats.Handle x)
            let mutable ingesterAccepting = true
            // 1. Submit to ingester until read queue, tranche limit or ingester limit exhausted
            while pending.Count <> 0 && write.HasCapacity && ingesterAccepting do
                let markCompleted, events = pending.Peek()
                let! submitted = ingester.TrySubmit(markCompleted, events)
                if submitted then
                    pending.Dequeue() |> ignore
                    // mark off a write as being in progress
                    do! write.Await()
                else
                    ingesterAccepting <- false 
            // 2. Update any progress into the stats
            stats.HandleValidated(Option.map fst validatedPos, fst read.State)
            validatedPos |> Option.iter progressWriter.Post
            stats.HandleCommitted progressWriter.CommittedEpoch
            // 3. Forward content for any active streams into processor immediately
            let relevantBufferedStreams = streams.Take(ingester.AllStreams.Contains)
            ingester.AddActiveStreams(relevantBufferedStreams)
            // 4. Periodically emit status info
            stats.TryDump(write.State,streams)
            do! Async.Sleep pumpDelayMs }

    /// Awaits space in `read` to limit reading ahead - yields present state of Read and Write phases
    member __.Submit(epoch, markBatchCompleted, events) = async {
        do! read.Await()
        work.Enqueue <| Add (epoch, markBatchCompleted, events)
        return read.State }

    static member Start<'R>(log, ingester, maxRead, maxWrite, statsInterval) =
        let instance = new TrancheEngine<'R>(log, ingester, maxRead, maxWrite, statsInterval)
        Async.Start <| instance.Pump()
        instance

    member __.Stop() =
        cts.Cancel()