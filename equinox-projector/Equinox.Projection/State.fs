﻿module Equinox.Projection.State

open Serilog
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Collections.Concurrent

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
let mb x = float x / 1024. / 1024.
let category (streamName : string) = streamName.Split([|'-'|],2).[0]

type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
type [<NoComparison>] StreamSpan = { stream: string; span: Span }
type [<NoComparison>] StreamState = { isMalformed: bool; write: int64 option; queue: Span[] } with
    member __.Size =
        if __.queue = null then 0
        else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy (fun x -> arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16)
    member __.TryGap() =
        if __.queue = null then None
        else
            match __.write, Array.tryHead __.queue with
            | Some w, Some { index = i } when i > w -> Some (w, i-w)
            | _ -> None
    member __.IsReady =
        if __.queue = null || __.isMalformed then false
        else
            match __.write, Array.tryHead __.queue with
            | Some w, Some { index = i } -> i = w
            | None, _ -> true
            | _ -> false

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
        { write = writePos; queue = Span.merge (defaultArg writePos 0L) items; isMalformed = s1.isMalformed || s2.isMalformed }

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
    let mutable streams = Set.empty 
    let states = Dictionary<string, StreamState>()
    let update stream (state : StreamState) =
        match states.TryGetValue stream with
        | false, _ ->
            states.Add(stream, state)
            streams <- streams.Add stream
            stream, state
        | true, current ->
            let updated = StreamState.combine current state
            states.[stream] <- updated
            stream, updated
    let updateWritePos stream isMalformed pos span = update stream { isMalformed = isMalformed; write = pos; queue = span }
    let markCompleted stream index = updateWritePos stream false (Some index) null |> ignore

    let busy = HashSet<string>()
    let schedule (requestedOrder : string seq) (capacity: int) =
        let toSchedule = ResizeArray<_>(capacity)
        let xs = requestedOrder.GetEnumerator()
        let mutable remaining = capacity
        while xs.MoveNext() && remaining <> 0 do
            let x = xs.Current
            let state = states.[x]
            if not state.isMalformed && busy.Add x then
                let q = state.queue
                if q = null then Log.Warning("Attempt to request scheduling for completed {stream} that has no items queued", x)
                toSchedule.Add(state.write, { stream = x; span = q.[0] })
                remaining <- remaining - 1
        toSchedule.ToArray()
    let markNotBusy stream =
        busy.Remove stream |> ignore

    member __.All = streams
    member __.InternalMerge(stream, state) = update stream state |> ignore
    member __.InternalUpdate stream pos queue = update stream { isMalformed = false; write = Some pos; queue = queue }
    member __.Add(stream, index, event, ?isMalformed) =
        updateWritePos stream (defaultArg isMalformed false) None [| { index = index; events = [| event |] } |]
    member __.Add(batch: StreamSpan, isMalformed) =
        updateWritePos batch.stream isMalformed None [| { index = batch.span.index; events = batch.span.events } |]
    member __.SetMalformed(stream,isMalformed) =
        updateWritePos stream isMalformed None [| { index = 0L; events = null } |]
    // DEPRECATED - will be removed
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
    member __.Schedule(requestedOrder : string seq, capacity: int) : (int64 option * StreamSpan)[] =
        schedule requestedOrder capacity
    member __.Dump(log : ILogger) =
        let mutable busyCount, busyB, ready, readyB, malformed, malformedB, synced = 0, 0L, 0, 0L, 0, 0L, 0
        let busyCats, readyCats, readyStreams, malformedStreams = CatStats(), CatStats(), CatStats(), CatStats()
        for KeyValue (stream,state) in states do
            match int64 state.Size with
            | 0L ->
                synced <- synced + 1
            | sz when state.isMalformed ->
                malformedStreams.Ingest(stream, mb sz |> int64)
                malformed <- malformed + 1
                malformedB <- malformedB + sz
            | sz when busy.Contains stream ->
                busyCats.Ingest(category stream)
                busyCount <- busyCount + 1
                busyB <- busyB + sz
            | sz ->
                readyCats.Ingest(category stream)
                readyStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, (sz + 512L) / 1024L)
                ready <- ready + 1
                readyB <- readyB + sz
        log.Information("Streams Synced {synced:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
            synced, busyCount, mb busyB, ready, mb readyB, malformed, mb malformedB)
        if busyCats.Any then log.Information("Active Categories, events {busyCats}", Seq.truncate 5 busyCats.StatsDescending)
        if readyCats.Any then log.Information("Ready Categories, events {readyCats}", Seq.truncate 5 readyCats.StatsDescending)
        if readyCats.Any then log.Information("Ready Streams, KB {readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
        if malformedStreams.Any then log.Information("Malformed Streams, MB {malformedStreams}", malformedStreams.StatsDescending)

    // Used to trigger catch-up reading of streams which have events missing prior to the observed write position
    //member __.TryGap() : (string*int64*int) option =
    //    let rec aux () =
    //        match gap |> Queue.tryDequeue with
    //        | None -> None
    //        | Some stream ->
            
    //        match states.[stream].TryGap() with
    //        | Some (pos,count) -> Some (stream,pos,int count)
    //        | None -> aux ()
    //    aux ()

type [<NoComparison; NoEquality>] internal BatchState = { markCompleted: unit -> unit; streamToRequiredIndex : Dictionary<string,int64> }

type ProgressState<'Pos>() =
    let pending = Queue<_>()
    member __.AppendBatch(markCompleted, reqs : Dictionary<string,int64>) =
        pending.Enqueue { markCompleted = markCompleted; streamToRequiredIndex = reqs }
    member __.MarkStreamProgress(stream, index) =
        for x in pending do
            match x.streamToRequiredIndex.TryGetValue stream with
            | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
            | _, _ -> ()
        let headIsComplete () = pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0
        let mutable completed = 0
        while headIsComplete () do
            let item = pending.Dequeue() in item.markCompleted()
            completed <- completed + 1
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
    member __.Validate tryGetStreamWritePos =
        let rec aux completed =
            if pending.Count = 0 then completed else
            let batch = pending.Peek()
            for KeyValue (stream, requiredIndex) in Array.ofSeq batch.streamToRequiredIndex do
                match tryGetStreamWritePos stream with
                | Some index when requiredIndex <= index ->
                    Log.Warning("Validation had to remove {stream} as required {req} has been met by {index}", stream, requiredIndex, index)
                    batch.streamToRequiredIndex.Remove stream |> ignore
                | _ -> ()
            if batch.streamToRequiredIndex.Count <> 0 then
                completed
            else
                let item = pending.Dequeue() in item.markCompleted()
                aux (completed + 1)
        aux 0

/// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
type Dispatcher<'R>(maxDop) =
    let work = new BlockingCollection<_>(ConcurrentQueue<_>())
    let result = Event<'R>()
    let dop = new SemaphoreSlim(maxDop)
    let dispatch work = async {
        let! res = work
        result.Trigger res
        dop.Release() |> ignore } 
    [<CLIEvent>] member __.Result = result.Publish
    member __.AvailableCapacity =
        let available = dop.CurrentCount
        available,maxDop
    member __.TryAdd(item,?timeout) = async {
        let! got = dop.Await(?timeout=timeout)
        if got then
            work.Add(item)
        return got }
    member __.Pump () = async {
        let! ct = Async.CancellationToken
        for item in work.GetConsumingEnumerable ct do
            Async.Start(dispatch item) }