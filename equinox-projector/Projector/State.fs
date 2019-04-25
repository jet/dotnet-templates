module ProjectorTemplate.Projector.State

open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading

module Metrics =
    module RuCounters =
        open Equinox.Cosmos.Store
        open Serilog.Events

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

    let dumpRuStats duration (log: Serilog.ILogger) =
        let stats =
          [ "Read", RuCounters.RuCounterSink.Read
            "Write", RuCounters.RuCounterSink.Write
            "Resync", RuCounters.RuCounterSink.Resync ]
        let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
        let logActivity name count rc lat =
            if count <> 0L then
                log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                    name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
        for name, stat in stats do
            let ru = float stat.rux100 / 100.
            totalCount <- totalCount + stat.count
            totalRc <- totalRc + ru
            totalMs <- totalMs + stat.ms
            logActivity name stat.count ru stat.ms
        logActivity "TOTAL" totalCount totalRc totalMs
        // Yes, there's a minor race here!
        RuCounters.RuCounterSink.Reset()
        let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
        let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
        for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)

module ConcurrentQueue =
    let drain handle (xs : ConcurrentQueue<_>) = 
        let rec aux () =
            match xs.TryDequeue() with
            | true, x -> handle x; aux ()
            | false, _ -> ()
        aux ()

let every ms f =
    let timer = Stopwatch.StartNew()
    fun () ->
        if timer.ElapsedMilliseconds > ms then
            f ()
            timer.Restart()
let expiredMs ms =
    let timer = Stopwatch.StartNew()
    fun () ->
        let due = timer.ElapsedMilliseconds > ms
        if due then timer.Restart()
        due

let arrayBytes (x:byte[]) = if x = null then 0 else x.Length
let private mb x = float x / 1024. / 1024.
let category (streamName : string) = streamName.Split([|'-'|],2).[0]

type [<NoComparison>] StreamItem = { stream: string; index: int64; event: Equinox.Codec.IEvent<byte[]> }
type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
type [<NoComparison>] StreamBatch = { stream: string; span: Span }
type [<NoComparison>] StreamState = { write: int64 option; queue: Span[] } with
    member __.Size =
        if __.queue = null then 0
        else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy (fun x -> arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16)

module StreamState =
    let (|NNA|) xs = if xs = null then Array.empty else xs
    module Span =
        let (|End|) (x : Span) = x.index + if x.events = null then 0L else x.events.LongLength
        let trim min = function
            | x when x.index >= min -> x // don't adjust if min not within
            | End n when n < min -> { index = min; events = [||] } // throw away if before min
            | x -> { index = min; events = x.events |> Array.skip (min - x.index |> int) }  // slice
        let merge min (xs : Span seq) =
            let xs =
                seq { for x in xs -> { x with events = (|NNA|) x.events } }
                |> Seq.map (trim min)
                |> Seq.filter (fun x -> x.events.Length <> 0)
                |> Seq.sortBy (fun x -> x.index)
            let buffer = ResizeArray()
            let mutable curr = None
            for x in xs do
                match curr, x with
                // Not overlapping, no data buffered -> buffer
                | None, _ ->
                    curr <- Some x
                // Gap
                | Some (End nextIndex as c), x when x.index > nextIndex ->
                    buffer.Add c
                    curr <- Some x
                // Overlapping, join
                | Some (End nextIndex as c), x  ->
                    curr <- Some { c with events = Array.append c.events (trim nextIndex x).events }
            curr |> Option.iter buffer.Add
            if buffer.Count = 0 then null else buffer.ToArray()

    let inline optionCombine f (r1: int64 option) (r2: int64 option) =
        match r1, r2 with
        | Some x, Some y -> f x y |> Some
        | None, None -> None
        | None, x | x, None -> x
    let combine (s1: StreamState) (s2: StreamState) : StreamState =
        let writePos = optionCombine max s1.write s2.write
        let items = let (NNA q1, NNA q2) = s1.queue, s2.queue in Seq.append q1 q2
        { write = writePos; queue = Span.merge (defaultArg writePos 0L) items }

/// Gathers stats relating to how many items of a given category have been observed
type CatStats() =
    let cats = Dictionary<string,int64>()
    member __.Ingest(cat,?weight) = 
        let weight = defaultArg weight 1L
        match cats.TryGetValue cat with
        | true, catCount -> cats.[cat] <- catCount + weight
        | false, _ -> cats.[cat] <- weight
    member __.Any = cats.Count <> 0
    member __.Clear() = cats.Clear()
    member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd

type StreamStates() =
    let states = Dictionary<string, StreamState>()
    let update stream (state : StreamState) =
        match states.TryGetValue stream with
        | false, _ ->
            states.Add(stream, state)
            stream, state
        | true, current ->
            let updated = StreamState.combine current state
            states.[stream] <- updated
            stream, updated
    let updateWritePos stream pos span = update stream { write = pos; queue = span }
    let markCompleted stream index = updateWritePos stream (Some index) null |> ignore
    let enqueue (item : StreamItem) = updateWritePos item.stream None [| { index = item.index; events = [| item.event |]}|]

    let busy = HashSet<string>()
    let schedule (requestedOrder : string seq) (capacity: int) =
        let toSchedule = ResizeArray<_>(capacity)
        let xs = requestedOrder.GetEnumerator()
        while xs.MoveNext() && toSchedule.Capacity <> 0 do
            let x = xs.Current
            if busy.Add x then
                let q = states.[x].queue
                if q = null then Log.Warning("Attempt to request scheduling for completed {stream} that has no items queued", x)
                toSchedule.Add { stream = x; span = q.[0] }
        toSchedule.ToArray()
    let markNotBusy stream =
        busy.Remove stream |> ignore

    //member __.Add(item: StreamBatch) = enqueue item
    member __.Add(item: StreamItem) = enqueue item |> ignore
    member __.TryGetStreamWritePos stream =
        match states.TryGetValue stream with
        | true, value -> value.write
        | false, _ -> None
    member __.QueueLength(stream) =
        let q = states.[stream].queue
        if q = null then Log.Warning("Attempt to request scheduling for completed {stream} that has no items queued", stream)
        q.[0].events.Length
    member __.MarkCompleted(stream, index) =
        markNotBusy stream
        markCompleted stream index
    member __.MarkFailed stream =
        markNotBusy stream
    member __.Schedule(requestedOrder : string seq, capacity: int) : StreamBatch[] =
        schedule requestedOrder capacity
    member __.Dump(log : ILogger) =
        let mutable busyCount, busyB, ready, readyB, synced = 0, 0L, 0, 0L, 0
        let busyCats, readyCats, readyStreams = CatStats(), CatStats(), CatStats()
        for KeyValue (stream,state) in states do
            match int64 state.Size with
            | 0L ->
                synced <- synced + 1
            | sz when busy.Contains stream ->
                busyCats.Ingest(category stream)
                busyCount <- busyCount + 1
                busyB <- busyB + sz
            | sz ->
                readyCats.Ingest(category stream)
                readyStreams.Ingest(sprintf "%s@%d" stream (defaultArg state.write 0L), mb sz |> int64)
                ready <- ready + 1
                readyB <- readyB + sz
        log.Information("Busy {busy}/{busyMb:n1}MB Ready {ready}/{readyMb:n1}MB Synced {synced}", busyCount, mb busyB, ready, mb readyB, synced)
        if busyCats.Any then log.Information("Busy Categories, events {busyCats}", busyCats.StatsDescending)
        if readyCats.Any then log.Information("Ready Categories, events {readyCats}", readyCats.StatsDescending)
        if readyCats.Any then log.Information("Ready Streams, MB {readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)

type [<NoComparison>] internal Chunk<'Pos> = { pos: 'Pos; streamToRequiredIndex : Dictionary<string,int64> }

type ProgressState<'Pos>(?currentPos : 'Pos) =
    let pending = Queue<_>()
    let mutable validatedPos = currentPos
    member __.AppendBatch(pos, reqs : Dictionary<string,int64>) =
        pending.Enqueue { pos = pos; streamToRequiredIndex = reqs }
    member __.MarkStreamProgress(stream, index) =
        for x in pending do
            match x.streamToRequiredIndex.TryGetValue stream with
            | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
            | _, _ -> ()
        let headIsComplete () =
            match pending.TryPeek() with
            | true, batch -> batch.streamToRequiredIndex.Count = 0
            | _ -> false
        let mutable completed = 0
        while headIsComplete () do
            completed <- completed + 1
            let headBatch = pending.Dequeue()
            validatedPos <- Some headBatch.pos
        completed
    member __.ScheduledOrder getStreamQueueLength =
        let raw = seq {
            let streams = HashSet()
            let mutable batch = 0
            for x in pending do
                batch <- batch + 1
                for s in x.streamToRequiredIndex.Keys do
                    if streams.Add s then
                        yield s,(batch,getStreamQueueLength s) }
        raw |> Seq.sortBy (fun (_s,(b,l)) -> b,-l) |> Seq.map fst
    member __.Validate tryGetStreamWritePos : 'Pos option * int =
        let rec aux () =
            match pending.TryPeek() with
            | false, _ -> ()
            | true, batch ->
                for KeyValue (stream, requiredIndex) in Array.ofSeq batch.streamToRequiredIndex do
                    match tryGetStreamWritePos stream with
                    | Some index when requiredIndex <= index ->
                        Log.Warning("Validation had to remove {stream}", stream)
                        batch.streamToRequiredIndex.Remove stream |> ignore
                    | _ -> ()
                if batch.streamToRequiredIndex.Count = 0 then
                    let headBatch = pending.Dequeue()
                    validatedPos <- Some headBatch.pos
                    aux ()
        aux ()
        validatedPos, pending.Count

/// Manages writing of progress
/// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
/// - retries until success or a new item is posted
type ProgressWriter() =
    let pumpSleepMs = 100
    let due = expiredMs 5000L
    let mutable committedEpoch = None
    let mutable validatedPos = None
    let result = Event<_>()
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
type CoordinatorWork =
    /// Enqueue a batch of items with supplied tag and progress marking function
    | Add of epoch: int64 * markCompleted: Async<unit> * items: StreamItem seq
    /// Log stats about an ingested batch
    | Added of streams: int * events: int
    /// Result of processing on stream - processed up to nominated `index` or threw `exn`
    | Result of stream: string * outcome: Choice<int64,exn>
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Choice<int64,exn>
    
type CoordinatorStats(log : ILogger, maxPendingBatches, ?statsInterval) =
    let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 1.)
    let mutable pendingBatchCount, validatedEpoch, comittedEpoch : int * int64 option * int64 option = 0, None, None
    let progCommitFails, progCommits = ref 0, ref 0 
    let cycles, batchesPended, streamsPended, eventsPended, resultOk, resultExn = ref 0, ref 0, ref 0, ref 0, ref 0, ref 0
    let statsDue = expiredMs (int64 statsInterval.TotalMilliseconds)
    let dumpStats (busy,capacity) (streams : StreamStates) =
        if !progCommitFails <> 0 || !progCommits <> 0 then
            match comittedEpoch with
            | None ->
                log.Error("Progress @ {validated}; writing failing: {failures} failures ({commits} successful commits) Uncommitted {pendingBatches}/{maxPendingBatches}",
                        Option.toNullable validatedEpoch, !progCommitFails, !progCommits, pendingBatchCount, maxPendingBatches)
            | Some committed when !progCommitFails <> 0 ->
                log.Warning("Progress @ {validated} (committed: {committed}, {commits} commits, {failures} failures) Uncommitted {pendingBatches}/{maxPendingBatches}",
                        Option.toNullable validatedEpoch, committed, !progCommits, !progCommitFails, pendingBatchCount, maxPendingBatches)
            | Some committed ->
                log.Information("Progress @ {validated} (committed: {committed}, {commits} commits) Uncommitted {pendingBatches}/{maxPendingBatches}",
                        Option.toNullable validatedEpoch, committed, !progCommits, pendingBatchCount, maxPendingBatches)
            progCommits := 0; progCommitFails := 0
        else
            log.Information("Progress @ {validated} (committed: {committed}) Uncommitted {pendingBatches}/{maxPendingBatches}",
                Option.toNullable validatedEpoch, Option.toNullable comittedEpoch, pendingBatchCount, maxPendingBatches)
        log.Information("Cycles {cycles} Ingested {batches} ({streams}s {events}e) Busy {busy}/{processors} Completed {completed} ({ok} ok {exns} exn)",
            !cycles, !batchesPended, !streamsPended, !eventsPended, busy, capacity, !resultOk + !resultExn, !resultOk, !resultExn)
        cycles := 0; batchesPended := 0; streamsPended := 0; eventsPended := 0; resultOk := 0; resultExn:= 0
        //Metrics.dumpRuStats statsInterval log
        streams.Dump log
    member __.Handle = function
        | Add _ -> ()
        | Added (streams, events) ->
            incr batchesPended
            eventsPended := !eventsPended + events
            streamsPended := !streamsPended + streams
        | Result (_stream, Choice1Of2 _) ->
            incr resultOk
        | Result (_stream, Choice2Of2 _) ->
            incr resultExn
        | ProgressResult (Choice1Of2 epoch) ->
            incr progCommits
            comittedEpoch <- Some epoch
        | ProgressResult (Choice2Of2 (_exn : exn)) ->
            incr progCommitFails
    member __.HandleValidated(epoch, pendingBatches) = 
        incr cycles
        pendingBatchCount <- pendingBatches
        validatedEpoch <- epoch
    member __.HandleCommitted epoch = 
        comittedEpoch <- epoch
    member __.TryDump(busy,capacity,streams) =
        if statsDue () then
            dumpStats (busy,capacity) streams

/// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
type Dispatcher(maxDop) =
    let cancellationCheckInterval = TimeSpan.FromMilliseconds 5.
    let work = new BlockingCollection<_>(ConcurrentQueue<_>())
    let result = Event<_>()
    let dop = new SemaphoreSlim(maxDop)
    let dispatch work = async {
        let! res = work
        result.Trigger res
        dop.Release() |> ignore } 
    [<CLIEvent>] member __.Result = result.Publish
    member __.Capacity = dop.CurrentCount
    member __.Enqueue item = work.Add item
    member __.Pump () = async {
        let! ct = Async.CancellationToken
        while not ct.IsCancellationRequested do
            let! got = dop.Await(cancellationCheckInterval)
            if got then
                let mutable item = Unchecked.defaultof<Async<_>>
                if work.TryTake(&item, cancellationCheckInterval) then Async.Start(dispatch item)
                else dop.Release() |> ignore }

/// Single instance per ChangeFeedObserver, spun up as leases are won and allocated by the ChangeFeedProcessor hosting framework
/// Coordinates a) ingestion of events b) execution of projection work c) writing of progress d) reporting of state
type Coordinator(log : ILogger, maxPendingBatches, processorDop, ?statsInterval) =
    let sleepIntervalMs = 5
    let cts = new CancellationTokenSource()
    let batches = new SemaphoreSlim(maxPendingBatches)
    let stats = CoordinatorStats(log, maxPendingBatches, ?statsInterval=statsInterval)
    let progressWriter = ProgressWriter()
    let progressState = ProgressState()
    let streams = StreamStates()
    let work = ConcurrentQueue<_>()
    let handle = function
        | Add (epoch, checkpoint,items) ->
            let reqs = Dictionary()
            let mutable count = 0
            for item in items do
                streams.Add item
                count <- count + 1
                reqs.[item.stream] <- item.index + 1L
            progressState.AppendBatch((epoch,checkpoint),reqs)
            work.Enqueue(Added (reqs.Count,count))
        | Added _ -> ()
        | Result (stream, Choice1Of2 index) ->
            let batchesCompleted = progressState.MarkStreamProgress(stream,index)
            if batchesCompleted <> 0 then batches.Release(batchesCompleted) |> ignore
            streams.MarkCompleted(stream,index)
        | Result (stream, Choice2Of2 _) ->
            streams.MarkFailed stream
        | ProgressResult _ -> ()

    member private __.Pump(project : StreamBatch -> Async<int>) = async {
        let dispatcher = Dispatcher(processorDop)
        use _ = progressWriter.Result.Subscribe(ProgressResult >> work.Enqueue)
        use _ = dispatcher.Result.Subscribe(Result >> work.Enqueue)
        Async.Start(progressWriter.Pump(), cts.Token)
        Async.Start(dispatcher.Pump(), cts.Token)
        while not cts.IsCancellationRequested do
            // 1. propagate read items to buffer; propagate write write results to buffer and progress write impacts to local state
            work |> ConcurrentQueue.drain (fun x -> handle x; stats.Handle x)
            // 2. Mark off any progress achieved (releasing memory and/or or unblocking reading of batches)
            let validatedPos, batches = progressState.Validate(streams.TryGetStreamWritePos)
            stats.HandleValidated(Option.map fst validatedPos, batches)
            validatedPos |> Option.iter progressWriter.Post
            stats.HandleCommitted progressWriter.CommittedEpoch
            // 3. After that, provision writers queue
            let capacity = dispatcher.Capacity
            if capacity <> 0 then
                let work = streams.Schedule(progressState.ScheduledOrder streams.QueueLength, capacity)
                for batch in work do
                    dispatcher.Enqueue <| async {
                        try let! count = project batch
                            return batch.stream, Choice1Of2 (batch.span.index + int64 count)
                        with e -> return batch.stream, Choice2Of2 e }
            // 4. Periodically emit status info
            let busy = processorDop - dispatcher.Capacity
            stats.TryDump(busy,processorDop,streams)
            do! Async.Sleep sleepIntervalMs }
    static member Start(rangeLog, maxPendingBatches, processorDop, project) =
        let instance = new Coordinator(rangeLog, maxPendingBatches, processorDop)
        Async.Start <| instance.Pump(project)
        instance
    member __.Submit(epoch, markBatchCompleted, events) = async {
        let! _ = batches.Await()
        Add (epoch, markBatchCompleted, Array.ofSeq events) |> work.Enqueue
        return maxPendingBatches-batches.CurrentCount,maxPendingBatches }

    member __.Stop() =
        cts.Cancel()