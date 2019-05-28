namespace Equinox.Projection

open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading

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
#if NET461
    member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortBy (fun (_,s) -> -s)
#else
    member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd
#endif

[<AutoOpen>]
module private Impl =
    let (|NNA|) xs = if xs = null then Array.empty else xs
    let arrayBytes (x:byte[]) = if x = null then 0 else x.Length
    let inline eventSize (x : Equinox.Codec.IEvent<_>) = arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16
    let inline mb x = float x / 1024. / 1024.
    let expiredMs ms =
        let timer = Stopwatch.StartNew()
        fun () ->
            let due = timer.ElapsedMilliseconds > ms
            if due then timer.Restart()
            due
    let inline accStopwatch (f : unit -> 't) at =
        let sw = Stopwatch.StartNew()
        let r = f ()
        at sw.Elapsed
        r

#nowarn "52" // see tmp.Sort

module Progress =

    type [<NoComparison; NoEquality>] internal BatchState = { markCompleted: unit -> unit; streamToRequiredIndex : Dictionary<string,int64> }

    type State<'Pos>() =
        let pending = Queue<_>()
        let trim () =
            while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                let batch = pending.Dequeue()
                batch.markCompleted()
        member __.AppendBatch(markCompleted, reqs : Dictionary<string,int64>) =
            pending.Enqueue { markCompleted = markCompleted; streamToRequiredIndex = reqs }
            trim ()
        member __.MarkStreamProgress(stream, index) =
            for x in pending do
                match x.streamToRequiredIndex.TryGetValue stream with
                | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
                | _, _ -> ()
            trim ()
        member __.InScheduledOrder getStreamWeight =
            let streams = HashSet()
            let tmp = ResizeArray(16384)
            let mutable batch = 0
            for x in pending do
                batch <- batch + 1
                for s in x.streamToRequiredIndex.Keys do
                    if streams.Add s then
                        tmp.Add((s,(batch,-getStreamWeight s)))
            let c = Comparer<_>.Default
            tmp.Sort(fun (_,_a) ((_,_b)) -> c.Compare(_a,_b))
            tmp |> Seq.map (fun ((s,_)) -> s)

module Buffering =

    /// Item from a reader as supplied to the `IIngester`
    type [<NoComparison>] StreamItem = { stream: string; index: int64; event: Equinox.Codec.IEvent<byte[]> }

    type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
    module Span =
        let (|End|) (x : Span) = x.index + if x.events = null then 0L else x.events.LongLength
        let dropBeforeIndex min : Span -> Span = function
            | x when x.index >= min -> x // don't adjust if min not within
            | End n when n < min -> { index = min; events = [||] } // throw away if before min
#if NET461
            | x -> { index = min; events = x.events |> Seq.skip (min - x.index |> int) |> Seq.toArray }
#else
            | x -> { index = min; events = x.events |> Array.skip (min - x.index |> int) }  // slice
#endif
        let merge min (xs : Span seq) =
            let xs =
                seq { for x in xs -> { x with events = (|NNA|) x.events } }
                |> Seq.map (dropBeforeIndex min)
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
                    curr <- Some { c with events = Array.append c.events (dropBeforeIndex nextIndex x).events }
            curr |> Option.iter buffer.Add
            if buffer.Count = 0 then null else buffer.ToArray()
        let slice (maxEvents,maxBytes) (x: Span) =
            let inline arrayBytes (x:byte[]) = if x = null then 0 else x.Length
            // TODO tests etc
            let inline estimateBytesAsJsonUtf8 (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + (x.EventType.Length * 2) + 96
            let mutable count,bytes = 0, 0
            let mutable countBudget, bytesBudget = maxEvents,maxBytes
            let withinLimits (y : Equinox.Codec.IEvent<byte[]>) =
                countBudget <- countBudget - 1
                let eventBytes = estimateBytesAsJsonUtf8 y
                bytesBudget <- bytesBudget - eventBytes
                // always send at least one event in order to surface the problem and have the stream marked malformed
                let res = count = 0 || (countBudget >= 0 && bytesBudget >= 0)
                if res then count <- count + 1; bytes <- bytes + eventBytes
                res
            let trimmed = { x with events = x.events |> Array.takeWhile withinLimits }
            let stats = trimmed.events.Length, trimmed.events |> Seq.sumBy estimateBytesAsJsonUtf8
            stats, trimmed
    type [<NoComparison>] StreamSpan = { stream: string; span: Span }
    type [<NoComparison>] StreamState = { isMalformed: bool; write: int64 option; queue: Span[] } with
        member __.Size =
            if __.queue = null then 0
            else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy eventSize
        member __.IsReady =
            if __.queue = null || __.isMalformed then false
            else
                match __.write, Array.tryHead __.queue with
                | Some w, Some { index = i } -> i = w
                | None, _ -> true
                | _ -> false
    module StreamState =
        let inline optionCombine f (r1: 'a option) (r2: 'a option) =
            match r1, r2 with
            | Some x, Some y -> f x y |> Some
            | None, None -> None
            | None, x | x, None -> x
        let combine (s1: StreamState) (s2: StreamState) : StreamState =
            let writePos = optionCombine max s1.write s2.write
            let items = let (NNA q1, NNA q2) = s1.queue, s2.queue in Seq.append q1 q2
            { write = writePos; queue = Span.merge (defaultArg writePos 0L) items; isMalformed = s1.isMalformed || s2.isMalformed }

    type Streams() =
        let states = Dictionary<string, StreamState>()
        let merge stream (state : StreamState) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
            | true, current ->
                let updated = StreamState.combine current state
                states.[stream] <- updated


        member __.Merge(item : StreamItem) =
            merge item.stream { isMalformed = false; write = None; queue = [| { index = item.index; events = [| item.event |] } |] }
        member __.Items : seq<KeyValuePair<string,StreamState>>= states :> _
        member __.Merge(other: Streams) =
            for x in other.Items do
                merge x.Key x.Value

        member __.Dump(categorize, log : ILogger) =
            let mutable waiting, waitingB = 0, 0L
            let waitingCats, waitingStreams = CatStats(), CatStats()
            for KeyValue (stream,state) in states do
                let sz = int64 state.Size
                waitingCats.Ingest(categorize stream)
                waitingStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, (sz + 512L) / 1024L)
                waiting <- waiting + 1
                waitingB <- waitingB + sz
            if waiting <> 0 then log.Information("Streams Waiting {busy:n0}/{busyMb:n1}MB ", waiting, mb waitingB)
            if waitingCats.Any then log.Information("Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information("Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

module Scheduling =

    open Buffering

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
        let updateWritePos stream isMalformed pos span = update stream { isMalformed = isMalformed; write = pos; queue = span }
        let markCompleted stream index = updateWritePos stream false (Some index) null |> ignore
        let merge (buffer : Streams) =
            for x in buffer.Items do
                update x.Key x.Value |> ignore

        let busy = HashSet<string>()
        let pending trySlipstreamed (requestedOrder : string seq) : seq<int64 option*StreamSpan> = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                let state = states.[s]
                if state.IsReady && not (busy.Contains s) then
                    proposed.Add s |> ignore
                    yield state.write, { stream = s; span = state.queue.[0] }
            if trySlipstreamed then
                // [lazily] Slipstream in futher events that have been posted to streams which we've already visited
                for KeyValue(s,v) in states do
                    if v.IsReady && not (busy.Contains s) && proposed.Add s then
                        yield v.write, { stream = s; span = v.queue.[0] } }
        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        member __.InternalMerge buffer = merge buffer
        member __.InternalUpdate stream pos queue = update stream { isMalformed = false; write = Some pos; queue = queue }
        //member __.Add(stream, index, event, ?isMalformed) =
        //    updateWritePos stream (defaultArg isMalformed false) None [| { index = index; events = [| event |] } |]
        //member __.Add(batch: StreamSpan, isMalformed) =
        //    updateWritePos batch.stream isMalformed None [| { index = batch.span.index; events = batch.span.events } |]
        member __.SetMalformed(stream,isMalformed) =
            updateWritePos stream isMalformed None [| { index = 0L; events = null } |]
        member __.QueueWeight(stream) =
            states.[stream].queue.[0].events |> Seq.sumBy eventSize
        member __.Item(stream) =
            states.[stream]
        member __.MarkBusy stream =
            markBusy stream
        member __.MarkCompleted(stream, index) =
            markNotBusy stream
            markCompleted stream index
        member __.MarkFailed stream =
            markNotBusy stream
        member __.Pending(trySlipstreamed, byQueuedPriority : string seq) : (int64 option * StreamSpan) seq =
            pending trySlipstreamed byQueuedPriority
        member __.Dump(log : ILogger, categorize) =
            let mutable busyCount, busyB, ready, readyB, unprefixed, unprefixedB, malformed, malformedB, synced = 0, 0L, 0, 0L, 0, 0L, 0, 0L, 0
            let busyCats, readyCats, readyStreams, unprefixedStreams, malformedStreams = CatStats(), CatStats(), CatStats(), CatStats(), CatStats()
            let kb sz = (sz + 512L) / 1024L
            for KeyValue (stream,state) in states do
                match int64 state.Size with
                | 0L ->
                    synced <- synced + 1
                | sz when busy.Contains stream ->
                    busyCats.Ingest(categorize stream)
                    busyCount <- busyCount + 1
                    busyB <- busyB + sz
                | sz when state.isMalformed ->
                    malformedStreams.Ingest(stream, mb sz |> int64)
                    malformed <- malformed + 1
                    malformedB <- malformedB + sz
                | sz when not state.IsReady ->
                    unprefixedStreams.Ingest(stream, mb sz |> int64)
                    unprefixed <- unprefixed + 1
                    unprefixedB <- unprefixedB + sz
                | sz ->
                    readyCats.Ingest(categorize stream)
                    readyStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, kb sz)
                    ready <- ready + 1
                    readyB <- readyB + sz
            log.Information("Streams Synced {synced:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
                synced, busyCount, mb busyB, ready, mb readyB, unprefixed, mb unprefixedB, malformed, mb malformedB)
            if busyCats.Any then log.Information("Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information("Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
            if readyCats.Any then log.Information("Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if unprefixedStreams.Any then log.Information("Waiting Streams, KB {@missingStreams}", Seq.truncate 3 unprefixedStreams.StatsDescending)
            if malformedStreams.Any then log.Information("Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)

    /// Messages used internally by projector, including synthetic ones for the purposes of the `Stats` listeners
    [<NoComparison; NoEquality>]
    type InternalMessage<'R> =
        /// Stats per submitted batch for stats listeners to aggregate
        | Added of streams: int * skip: int * events: int
        /// Result of processing on stream - result (with basic stats) or the `exn` encountered
        | Result of stream: string * outcome: 'R
       
    type BufferState = Idle | Busy | Full | Slipstreaming
    /// Gathers stats pertaining to the core projection/ingestion activity
    type Stats<'R,'E>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let states, fullCycles, cycles, resultCompleted, resultExn = CatStats(), ref 0, ref 0, ref 0, ref 0
        let batchesPended, streamsPended, eventsSkipped, eventsPended = ref 0, ref 0, ref 0, ref 0
        let statsDue, stateDue = expiredMs (int64 statsInterval.TotalMilliseconds), expiredMs (int64 stateInterval.TotalMilliseconds)
        let mutable dt,ft,it,st,mt = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
        let dumpStats (used,maxDop) waitingBatches =
            log.Information("Cycles {cycles}/{fullCycles} {@states} Projecting {busy}/{processors} Completed {completed} Exceptions {exns}",
                !cycles, !fullCycles, states.StatsDescending, used, maxDop, !resultCompleted, !resultExn)
            cycles := 0; fullCycles := 0; states.Clear(); resultCompleted := 0; resultExn:= 0
            log.Information("Batches Waiting {batchesWaiting} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e)",
                waitingBatches, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped)
            batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0
            log.Information("Scheduling Streams {mt:n1}s Batches {it:n1}s Dispatch {ft:n1}s Results {dt:n1}s Stats {st:n1}s",
                mt.TotalSeconds, it.TotalSeconds, ft.TotalSeconds, dt.TotalSeconds, st.TotalSeconds)
            dt <- TimeSpan.Zero; ft <- TimeSpan.Zero; it <- TimeSpan.Zero; st <- TimeSpan.Zero; mt <- TimeSpan.Zero
        abstract member Handle : InternalMessage<Choice<'R,'E>> -> unit
        default __.Handle msg = msg |> function
            | Added (streams, skipped, events) ->
                incr batchesPended
                streamsPended := !streamsPended + streams
                eventsPended := !eventsPended + events
                eventsSkipped := !eventsSkipped + skipped
            | Result (_stream, Choice1Of2 _) ->
                incr resultCompleted
            | Result (_stream, Choice2Of2 _) ->
                incr resultExn
        member __.DumpStats((used,max), pendingCount) =
            incr cycles
            if statsDue () then
                dumpStats (used,max) pendingCount
                __.DumpExtraStats()
        member __.TryDumpState(state,dump,(_dt,_ft,_mt,_it,_st)) =
            dt <- dt + _dt
            ft <- ft + _ft
            mt <- mt + _mt
            it <- it + _it
            st <- st + _st
            incr fullCycles
            states.Ingest(string state)
            
            let due = stateDue ()
            if due then
                dump log
            due
        /// Allows an ingester or projector to wire in custom stats (typically based on data gathered in a `Handle` override)
        abstract DumpExtraStats : unit -> unit
        default __.DumpExtraStats () = ()

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type Dispatcher<'R>(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag'sthread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>()) 
        let result = Event<'R>()
        let dop = new SemaphoreSlim(maxDop)
        let dispatch work = async {
            let! res = work
            result.Trigger res
            dop.Release() |> ignore } 
        [<CLIEvent>] member __.Result = result.Publish
        member __.HasCapacity = dop.CurrentCount > 0
        member __.State = maxDop-dop.CurrentCount,max 
        member __.TryAdd(item) =
            if dop.Wait 0 then
                work.Add(item)
                true
            else false
        member __.Pump () = async {
            let! ct = Async.CancellationToken
            for item in work.GetConsumingEnumerable ct do
                Async.Start(dispatch item) }

    [<NoComparison>]
    type Batch private (onCompletion, buffer, reqs) =
        let mutable buffer = Some buffer
        static member Create (onCompletion, items : StreamItem seq) =
            let buffer, reqs = Buffering.Streams(), Dictionary<string,int64>()
            for item in items do
                buffer.Merge(item)
                match reqs.TryGetValue(item.stream), item.index + 1L with
                | (false, _), required -> reqs.[item.stream] <- required
                | (true, actual), required when actual < required -> reqs.[item.stream] <- required
                | (true,_), _ -> () // replayed same or earlier item
            reqs.TrimExcess()
            Batch(onCompletion, buffer, reqs)
        member __.OnCompletion = onCompletion
        member __.Reqs = reqs :> seq<KeyValuePair<string,int64>>
        member __.ItemCount = reqs.Count
        member __.TryTakeStreams() = let t = buffer in buffer <- None; t
        member __.TryMerge(other : Batch) =
            match buffer, other.TryTakeStreams() with
            | Some x, Some y -> x.Merge(y); true
            | _, x -> buffer <- x; false

    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type SchedulingEngine<'R,'E>(dispatcher : Dispatcher<_>, stats : Stats<'R,'E>, project : int64 option * StreamSpan -> Async<Choice<_,_>>, interpretProgress, dumpStreams, ?maxBatches) =
        let sleepIntervalMs = 1
        let maxBatches = defaultArg maxBatches 4 
        let work = ConcurrentStack<InternalMessage<Choice<'R,'E>>>() // dont need them ordered so Queue is unwarranted; usage is cross-thread so Bag is not better
        let pending = ConcurrentQueue<Batch>() // Queue as need ordering
        let streams = StreamStates()
        let progressState = Progress.State()

        // ingest information to be gleaned from processing the results into `streams`
        static let workLocalBuffer = Array.zeroCreate 1024
        let tryDrainResults feedStats =
            let mutable worked, more = false, true
            while more do
                let c = work.TryPopRange(workLocalBuffer)
                if c = 0 (*&& work.IsEmpty*) then more <- false else worked <- true
                for i in 0..c-1 do
                    let x = workLocalBuffer.[i]
                    match x with
                    | Added _ -> () // Only processed in Stats (and actually never enters this queue)
                    | Result (stream,res) ->
                        match interpretProgress streams stream res with
                        | None -> streams.MarkFailed stream
                        | Some index ->
                            progressState.MarkStreamProgress(stream,index)
                            streams.MarkCompleted(stream,index)
                    feedStats x
            worked
        // On each iteration, we try to fill the in-flight queue, taking the oldest and/or heaviest streams first
        let tryFillDispatcher includeSlipstreamed =
            let mutable hasCapacity, dispatched = dispatcher.HasCapacity, false
            if hasCapacity then
                let potential = streams.Pending(includeSlipstreamed, progressState.InScheduledOrder streams.QueueWeight)
                let xs = potential.GetEnumerator()
                while xs.MoveNext() && hasCapacity do
                    let (_,{stream = s} : StreamSpan) as item = xs.Current
                    let succeeded = dispatcher.TryAdd(async { let! r = project item in return s, r })
                    if succeeded then streams.MarkBusy s
                    dispatched <- dispatched || succeeded // if we added any request, we'll skip sleeping
                    hasCapacity <- succeeded
            hasCapacity, dispatched
        // Take an incoming batch of events, correlating it against our known stream state to yield a set of remaining work
        let ingestPendingBatch feedStats (markCompleted, items : seq<KeyValuePair<string,int64>>) = 
            let inline validVsSkip (streamState : StreamState) required =
                match streamState.write with
                | Some cw when cw >= required -> 0, 1
                | _ -> 1, 0
            let reqs = Dictionary()
            let mutable count, skipCount = 0, 0
            for item in items do
                let streamState = streams.Item item.Key
                match validVsSkip streamState item.Value with
                | 0, skip ->
                    skipCount <- skipCount + skip
                | required, _ ->
                    count <- count + required
                    reqs.[item.Key] <- item.Value
            progressState.AppendBatch(markCompleted,reqs)
            feedStats <| Added (reqs.Count,skipCount,count)

        member __.Pump _abend = async {
            use _ = dispatcher.Result.Subscribe(Result >> work.Push)
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable idle, dispatcherState, remaining = true, Idle, 16
                let mutable dt,ft,mt,it,st = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
                while remaining <> 0 do
                    remaining <- remaining - 1
                    // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                    let processedResults = (fun () -> tryDrainResults stats.Handle) |> accStopwatch <| fun x -> dt <- dt + x
                    // 2. top up provisioning of writers queue
                    let hasCapacity, dispatched = (fun () -> tryFillDispatcher (dispatcherState = Slipstreaming)) |> accStopwatch <| fun x -> ft <- ft + x
                    idle <- idle && not processedResults && not dispatched
                    match dispatcherState with
                    | Idle when not hasCapacity ->
                        // If we've achieved full state, spin around the loop to dump stats and ingest reader data
                        dispatcherState <- Full
                        remaining <- 0
                    | Idle when remaining = 0 ->
                        dispatcherState <- Busy
                    | Idle -> // need to bring more work into the pool as we can't fill the work queue from what we have
                        // If we're going to fill the write queue with random work, we should bring all read events into the state first
                        // If we're going to bring in lots of batches, that's more efficient when the streamwise merges are carried out first
                        let mutable more, batchesTaken = true, 0
                        while more do
                            match pending.TryDequeue() with
                            | true, batch ->
                                match batch.TryTakeStreams () with None -> () | Some s -> (fun () -> streams.InternalMerge(s)) |> accStopwatch <| fun t -> mt <- mt + t
                                (fun () -> ingestPendingBatch stats.Handle (batch.OnCompletion, batch.Reqs)) |> accStopwatch <| fun t -> it <- it + t
                                batchesTaken <- batchesTaken + 1
                                more <- batchesTaken < maxBatches
                            | false,_ when batchesTaken <> 0 ->
                                more <- false
                            | false,_ when batchesTaken = 0 ->
                                dispatcherState <- Slipstreaming
                                more <- false
                            | false,_ -> ()
                    | Slipstreaming -> // only do one round of slipstreaming
                        remaining <- 0
                    | Busy | Full -> failwith "Not handled here"
                    // This loop can take a long time; attempt logging of stats per iteration
                    (fun () -> stats.DumpStats(dispatcher.State,pending.Count)) |> accStopwatch <| fun t -> st <- st + t
                // 3. Record completion state once per full iteration; dumping streams is expensive so needs to be done infrequently
                if not (stats.TryDumpState(dispatcherState,dumpStreams streams,(dt,ft,mt,it,st))) && not idle then
                    // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                    Thread.Sleep sleepIntervalMs } // Not Async.Sleep so we don't give up the thread
        member __.Submit(x : Batch) =
            pending.Enqueue(x)

    type Factory =
        static member Create(dispatcher : Dispatcher<_>, stats : Stats<int64,exn>, handle : StreamSpan -> Async<int>, dumpStreams, ?maxBatches) : SchedulingEngine<int64,exn> =
            let project (_maybeWritePos, batch : StreamSpan) = async {
                try let! count = handle batch
                    return Choice1Of2 (batch.span.index + int64 count)
                with e -> return Choice2Of2 e }
            let interpretProgress _streams _stream = function
                | Choice1Of2 index -> Some index
                | Choice2Of2 _ -> None

            SchedulingEngine<int64,exn>(dispatcher, stats, project, interpretProgress, dumpStreams, ?maxBatches = maxBatches)

type OrderedConsumer =
    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'M>
        (   log : ILogger, config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig, maxDop, enumStreamItems, handle, categorize,
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?stateInterval, ?logExternalStats) =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5

        let dispatcher = Scheduling.Dispatcher<_> maxDop
        let stats = Scheduling.Stats(log, statsInterval, stateInterval)
        let dumpStreams (streams : Scheduling.StreamStates) log =
            logExternalStats |> Option.iter (fun f -> f log)
            streams.Dump(log, categorize)
        let handle (x : Buffering.StreamSpan) = handle (x.stream, x.span)
        let scheduler = Scheduling.Factory.Create(dispatcher, stats, handle, dumpStreams)
        let mapBatch onCompletion (x : Jet.ConfluentKafka.FSharp.Submission.Batch<_>) : Scheduling.Batch =
            let onCompletion () = x.onCompletion(); onCompletion()
            Scheduling.Batch.Create(onCompletion, Seq.collect enumStreamItems x.messages)

        let tryCompactQueue (queue : Queue<Scheduling.Batch>) =
            let mutable acc, worked = None, false
            for x in queue do
                match acc with
                | None -> acc <- Some x
                | Some a -> if a.TryMerge x then worked <- true
            worked
        let submitBatch (x : Scheduling.Batch) : int =
            scheduler.Submit x
            x.ItemCount
        let submitter = Jet.ConfluentKafka.FSharp.Submission.SubmissionEngine(log, pumpInterval, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, tryCompactQueue)
        let mapResult (x : Confluent.Kafka.ConsumeResult<string,string>) = KeyValuePair(x.Key,x.Value)
        Jet.ConfluentKafka.FSharp.ParallelConsumer.Start(log, config, mapResult, submitter.Submit, submitter.Pump(), scheduler.Pump, dispatcher.Pump(), statsInterval)