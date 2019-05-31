namespace Jet.Projection

open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading

/// Item from a reader as supplied to the `IIngester`
type [<NoComparison>] StreamItem = { stream: string; index: int64; event: Equinox.Codec.IEvent<byte[]> }

type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }

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
    member __.StatsDescending = cats |> Seq.sortBy (fun x -> -x.Value) |> Seq.map (|KeyValue|)

/// Gathers stats relating to how many items of a given partition have been observed
type PartitionStats() =
    let partitions = Dictionary<int,int64>()
    member __.Record(partitionId, ?weight) = 
        let weight = defaultArg weight 1L
        match partitions.TryGetValue partitionId with
        | true, catCount -> partitions.[partitionId] <- catCount + weight
        | false, _ -> partitions.[partitionId] <- weight
    member __.Clear() = partitions.Clear()
    member __.StatsDescending = partitions |> Seq.sortBy (fun x -> -x.Value) |> Seq.map (|KeyValue|)

[<AutoOpen>]
module private Impl =
    let (|NNA|) xs = if xs = null then Array.empty else xs
    let arrayBytes (x:byte[]) = if x = null then 0 else x.Length
    let inline eventSize (x : Equinox.Codec.IEvent<_>) = arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16
    let inline mb x = float x / 1024. / 1024.
    /// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
    let intervalCheck (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        fun () ->
            let due = timer.ElapsedMilliseconds > max
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
            trim ()
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

    /// Manages writing of progress
    /// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
    /// - retries until success or a new item is posted
    type Writer<'Res when 'Res: equality>(?period,?sleep) =
        let writeInterval,sleepPeriod = defaultArg period (TimeSpan.FromSeconds 5.), int (defaultArg sleep (TimeSpan.FromMilliseconds 100.)).TotalMilliseconds
        let due = intervalCheck writeInterval
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
                | _ -> do! Async.Sleep sleepPeriod }

module Buffering =

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
    type [<NoComparison>] StreamState = { isMalformed: bool; write: int64 option; queue: Span[] } with
        member __.Size =
            if __.queue = null then 0
            else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy eventSize
        member __.HasValid = __.queue <> null && not __.isMalformed
        member __.IsReady =
            if not __.HasValid then false else

            match __.write, Array.head __.queue with
            | Some w, { index = i } -> i = w
            | None, _ -> true
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
            if waiting <> 0 then log.Information(" Streams Waiting {busy:n0}/{busyMb:n1}MB ", waiting, mb waitingB)
            if waitingCats.Any then log.Information(" Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information(" Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

module StreamScheduling =

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
        let pending trySlipstreamed (requestedOrder : string seq) : seq<int64 option*string*Span> = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                let state = states.[s]
                if state.HasValid && not (busy.Contains s) then
                    proposed.Add s |> ignore
                    yield state.write, s, state.queue.[0]
            if trySlipstreamed then
                // [lazily] Slipstream in further events that are not yet referenced by in-scope batches
                for KeyValue(s,v) in states do
                    if v.HasValid && not (busy.Contains s) && proposed.Add s then
                        yield v.write, s, v.queue.[0] }
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
        member __.Pending(trySlipstreamed, byQueuedPriority : string seq) : (int64 option * string * Span) seq =
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
            if busyCats.Any then log.Information(" Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
            if readyCats.Any then log.Information(" Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if unprefixedStreams.Any then log.Information(" Waiting Streams, KB {@missingStreams}", Seq.truncate 3 unprefixedStreams.StatsDescending)
            if malformedStreams.Any then log.Information(" Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)

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
        let statsDue, stateDue = intervalCheck statsInterval, intervalCheck stateInterval
        let mutable dt,ft,it,st,mt = TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero
        let dumpStats (dispatchActive,dispatchMax) batchesWaiting =
            log.Information("Scheduler {cycles} cycles ({fullCycles} full) {@states} Running {busy}/{processors} Completed {completed} Exceptions {exns}",
                !cycles, !fullCycles, states.StatsDescending, dispatchActive, dispatchMax, !resultCompleted, !resultExn)
            cycles := 0; fullCycles := 0; states.Clear(); resultCompleted := 0; resultExn:= 0
            log.Information(" Batches Holding {batchesWaiting} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e)",
                batchesWaiting, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped)
            batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0
            log.Information(" Cpu Streams {mt:n1}s Batches {it:n1}s Dispatch {ft:n1}s Results {dt:n1}s Stats {st:n1}s",
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
        member __.DumpStats((used,max), batchesWaiting) =
            incr cycles
            if statsDue () then
                dumpStats (used,max) batchesWaiting
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

    type Sem(max) =
        let inner = new SemaphoreSlim(max)
        member __.TryTake() = inner.Wait 0
        member __.HasCapacity = inner.CurrentCount <> 0
        member __.Release() = inner.Release() |> ignore
        member __.State = max-inner.CurrentCount,max

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type Dispatcher<'R>(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag'sthread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>()) 
        let result = Event<'R>()
        let dop = Sem maxDop
        let dispatch work = async {
            let! res = work
            result.Trigger res
            dop.Release() } 
        [<CLIEvent>] member __.Result = result.Publish
        member __.HasCapacity = dop.HasCapacity
        member __.State = dop.State
        member __.TryAdd(item) =
            if dop.TryTake() then
                work.Add(item)
                true
            else false
        member __.Pump () = async {
            let! ct = Async.CancellationToken
            for item in work.GetConsumingEnumerable ct do
                Async.Start(dispatch item) }

    [<NoComparison>]
    type StreamsBatch private (onCompletion, buffer, reqs) =
        let mutable buffer = Some buffer
        static member Create(onCompletion, items : StreamItem seq) =
            let buffer, reqs = Buffering.Streams(), Dictionary<string,int64>()
            let mutable itemCount = 0
            for item in items do
                itemCount <- itemCount + 1
                buffer.Merge(item)
                match reqs.TryGetValue(item.stream), item.index + 1L with
                | (false, _), required -> reqs.[item.stream] <- required
                | (true, actual), required when actual < required -> reqs.[item.stream] <- required
                | (true,_), _ -> () // replayed same or earlier item
            reqs.TrimExcess()
            let batch = StreamsBatch(onCompletion, buffer, reqs)
            batch,(batch.RemainingStreamsCount,itemCount)
        member __.OnCompletion = onCompletion
        member __.Reqs = reqs :> seq<KeyValuePair<string,int64>>
        member __.RemainingStreamsCount = reqs.Count
        member __.TryTakeStreams() = let t = buffer in buffer <- None; t
        member __.TryMerge(other : StreamsBatch) =
            match buffer, other.TryTakeStreams() with
            | Some x, Some y -> x.Merge(y); true
            | Some _, None -> false
            | None, x -> buffer <- x; false

    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type StreamSchedulingEngine<'R,'E>
        (   dispatcher : Dispatcher<_>, stats : Stats<'R,'E>, project : int64 option * string * Span -> Async<Choice<_,_>>, interpretProgress, dumpStreams,
            /// Tune number of batches to ingest at a time. Default 4
            ?maxBatches,
            /// Opt-in to allowing items to be processed independent of batch sequencing - requires upstream/projection function to be able to identify gaps
            ?enableSlipstreaming) =
        let sleepIntervalMs, maxBatches, slipstreamingEnabled = 2, defaultArg maxBatches 4, defaultArg enableSlipstreaming false
        let work = ConcurrentStack<InternalMessage<Choice<'R,'E>>>() // dont need them ordered so Queue is unwarranted; usage is cross-thread so Bag is not better
        let pending = ConcurrentQueue<StreamsBatch>() // Queue as need ordering
        let streams = StreamStates()
        let progressState = Progress.State()

        // ingest information to be gleaned from processing the results into `streams`
        static let workLocalBuffer = Array.zeroCreate 1024
        let tryDrainResults feedStats =
            let mutable worked, more = false, true
            while more do
                let c = work.TryPopRange(workLocalBuffer)
                if c = 0 (*&& work.IsEmpty*) then
                    more <- false
                else worked <- true
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
                    let (_,s,_) as item = xs.Current
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
                                match batch.TryTakeStreams() with None -> () | Some s -> (fun () -> streams.InternalMerge(s)) |> accStopwatch <| fun t -> mt <- mt + t
                                (fun () -> ingestPendingBatch stats.Handle (batch.OnCompletion, batch.Reqs)) |> accStopwatch <| fun t -> it <- it + t
                                batchesTaken <- batchesTaken + 1
                                more <- batchesTaken < maxBatches
                            | false,_ when batchesTaken <> 0  ->
                                more <- false
                            | false,_ when (*batchesTaken = 0 && *)slipstreamingEnabled ->
                                dispatcherState <- Slipstreaming
                                more <- false
                            | false,_  ->
                                remaining <- 0
                                more <- false
                    | Slipstreaming -> // only do one round of slipstreaming
                        remaining <- 0
                    | Busy | Full -> failwith "Not handled here"
                    // This loop can take a long time; attempt logging of stats per iteration
                    (fun () -> stats.DumpStats(dispatcher.State,pending.Count)) |> accStopwatch <| fun t -> st <- st + t
                // 3. Record completion state once per full iteration; dumping streams is expensive so needs to be done infrequently
                if not (stats.TryDumpState(dispatcherState,dumpStreams streams,(dt,ft,mt,it,st))) && not idle then
                    // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                    Thread.Sleep sleepIntervalMs } // Not Async.Sleep so we don't give up the thread
        member __.Submit(x : StreamsBatch) =
            pending.Enqueue(x)
    type StreamSchedulingEngine =
        static member Create
            (   dispatcher : Dispatcher<string*Choice<int64,exn>>, stats : Stats<int64,exn>, handle : string*Span -> Async<int>, dumpStreams,
                ?maxBatches, ?enableSlipstreaming)
            : StreamSchedulingEngine<int64,exn> =
            let project (_maybeWritePos, stream, span) : Async<Choice<int64,exn>> = async {
                try let! count = handle (stream,span)
                    return Choice1Of2 (span.index + int64 count)
                with e -> return Choice2Of2 e }
            let interpretProgress _streams _stream : Choice<int64,exn> -> Option<int64> = function
                | Choice1Of2 index -> Some index
                | Choice2Of2 _ -> None
            StreamSchedulingEngine(dispatcher, stats, project, interpretProgress, dumpStreams, ?maxBatches = maxBatches, ?enableSlipstreaming=enableSlipstreaming)

/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Submission =

    /// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type Batch<'M> = { partitionId : int; onCompletion: unit -> unit; messages: 'M [] }

    /// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
    [<NoComparison>]
    type PartitionQueue<'B> = { submissions: SemaphoreSlim; queue : Queue<'B> } with
        member __.Append(batch) = __.queue.Enqueue batch
        static member Create(maxSubmits) = { submissions = new SemaphoreSlim(maxSubmits); queue = Queue(maxSubmits * 2) } 

    /// Holds the stream of incoming batches, grouping by partition
    /// Manages the submission of batches into the Scheduler in a fair manner
    type SubmissionEngine<'M,'B>
        (   log : ILogger, maxSubmitsPerPartition, mapBatch: (unit -> unit) -> Batch<'M> -> 'B, submitBatch : 'B -> int, statsInterval, ?pumpInterval : TimeSpan,
            ?tryCompactQueue) =
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let incoming = new BlockingCollection<Batch<'M>[]>(ConcurrentQueue())
        let buffer = Dictionary<int,PartitionQueue<'B>>()
        let mutable cycles, ingested, compacted = 0, 0, 0
        let submittedBatches,submittedMessages = PartitionStats(), PartitionStats()
        let dumpStats () =
            let waiting = seq { for x in buffer do if x.Value.queue.Count <> 0 then yield x.Key, x.Value.queue.Count } |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Submitter {cycles} cycles {ingested} accepted {compactions} compactions Holding {@waiting}", cycles, ingested, compacted, waiting)
            log.Information(" Submitted Batches {@batches} Messages {@messages}", submittedBatches.StatsDescending, submittedMessages.StatsDescending)
            ingested <- 0; compacted <- 0; cycles <- 0; submittedBatches.Clear(); submittedMessages.Clear()
        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats ()
        // Loop, submitting 0 or 1 item per partition per iteration to ensure
        // - each partition has a controlled maximum number of entrants in the scheduler queue
        // - a fair ordering of batch submissions
        let propagate () =
            let mutable more, worked = true, false
            while more do
                more <- false
                for KeyValue(pi,pq) in buffer do
                    if pq.queue.Count <> 0 then
                        if pq.submissions.Wait(0) then
                            worked <- true
                            more <- true
                            let count = submitBatch <| pq.queue.Dequeue()
                            submittedBatches.Record(pi)
                            submittedMessages.Record(pi, int64 count)
            worked
        /// Take one timeslice worth of ingestion and add to relevant partition queues
        /// When ingested, we allow one propagation submission per partition
        let ingest (partitionBatches : Batch<'M>[]) =
            for { partitionId = pid } as batch in partitionBatches do
                let pq =
                    match buffer.TryGetValue pid with
                    | false, _ -> let t = PartitionQueue<_>.Create(maxSubmitsPerPartition) in buffer.[pid] <- t; t
                    | true, pq -> pq
                let mapped = mapBatch (fun () -> pq.submissions.Release() |> ignore) batch
                pq.Append(mapped)
            propagate()
        /// We use timeslices where we're we've fully provisioned the scheduler to index any waiting Batches
        let compact f =
            let mutable worked = false
            for KeyValue(_,pq) in buffer do
                if f pq.queue then
                    worked <- true
            if worked then compacted <- compacted + 1; true
            else false

        /// Processing loop, continuously splitting `Submit`ted items into per-partition queues and ensuring enough items are provided to the Scheduler
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable items = Unchecked.defaultof<_>
                let mutable propagated = false
                if incoming.TryTake(&items, pumpInterval) then
                    propagated <- ingest items
                    while incoming.TryTake(&items) do
                        if ingest items then propagated <- true
                else propagated <- propagate()
                match propagated, tryCompactQueue with
                | false, None -> Thread.Sleep 2
                | false, Some f when not (compact f) -> Thread.Sleep 2
                | _ -> ()

                maybeLogStats () }

        /// Supplies a set of Batches for holding and forwarding to scheduler at the right time
        member __.Ingest(items : Batch<'M>[]) =
            Interlocked.Increment(&ingested) |> ignore
            incoming.Add items

        /// Supplies an incoming Batch for holding and forwarding to scheduler at the right time
        member __.Ingest(batch : Batch<'M>) =
            __.Ingest(Array.singleton batch)