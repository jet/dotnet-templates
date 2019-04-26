module Equinox.Projection.Coordination

open Equinox.Projection.State
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

[<NoComparison; NoEquality>]
type Message<'R> =
    /// Enqueue a batch of items with supplied tag and progress marking function
    | Add of epoch: int64 * markCompleted: Async<unit> * items: StreamItem seq
    | AddStream of StreamSpan
    /// Log stats about an ingested batch
    | Added of streams: int * skip: int * events: int
    /// Result of processing on stream - specified number of items or threw `exn`
    | Result of stream: string * outcome: Choice<'R,exn>
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Choice<int64,exn>
    
type Stats<'R>(log : ILogger, maxPendingBatches, statsInterval : TimeSpan) =
    let mutable pendingBatchCount, validatedEpoch, comittedEpoch : int * int64 option * int64 option = 0, None, None
    let progCommitFails, progCommits = ref 0, ref 0 
    let cycles, batchesPended, streamsPended, eventsSkipped, eventsPended, resultCompleted, resultExn = ref 0, ref 0, ref 0, ref 0, ref 0, ref 0, ref 0
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
        log.Information("Cycles {cycles} Ingested {batches} ({streams}s {events}-{skipped}e) Busy {busy}/{processors} Completed {completed} ({passed} passed {exns} exn)",
            !cycles, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped, busy, capacity, !resultCompleted + !resultExn, !resultCompleted, !resultExn)
        cycles := 0; batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0; resultCompleted := 0; resultExn:= 0
        streams.Dump log
    abstract member Handle : Message<'R> -> unit
    default __.Handle res =
        match res with
        | Add _ | AddStream _ -> ()
        | Added (streams, skipped, events) ->
            incr batchesPended
            streamsPended := !streamsPended + streams
            eventsPended := !eventsPended + events
            eventsSkipped := !eventsSkipped + skipped
        | Result (_stream, Choice1Of2 _) ->
            incr resultCompleted
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
            __.DumpExtraStats()
    abstract DumpExtraStats : unit -> unit
    default __.DumpExtraStats () = ()

/// Single instance per Source; Coordinates
/// a) ingestion of events
/// b) execution of projection/ingestion work
/// c) writing of progress
/// d) reporting of state
/// The key bit that's managed externally is the reading/accepting of incoming data
type Coordinator<'R>(maxPendingBatches, processorDop, project : int64 option * StreamSpan -> Async<string * Choice<'R,exn>>, handleResult) =
    let sleepIntervalMs = 5
    let cts = new CancellationTokenSource()
    let batches = new SemaphoreSlim(maxPendingBatches)
    let work = ConcurrentQueue<Message<'R>>()
    let streams = StreamStates()
    let dispatcher = Dispatcher(processorDop)
    let progressState = ProgressState()
    let progressWriter = ProgressWriter<_>()

    member private __.Pump(stats : Stats<'R>) = async {
        use _ = progressWriter.Result.Subscribe(ProgressResult >> work.Enqueue)
        use _ = dispatcher.Result.Subscribe(Result >> work.Enqueue)
        Async.Start(progressWriter.Pump(), cts.Token)
        Async.Start(dispatcher.Pump(), cts.Token)
        let canSkip (streamState : StreamState) (item : StreamItem) =
            match streamState.write, item.index + 1L with
            | Some cw, required -> cw >= required
            | _ -> false

        let handle x =
            match x with
            | Add (epoch, checkpoint, items) ->
                let reqs = Dictionary()
                let mutable count, skipCount = 0, 0
                for item in items do
                    let streamState = streams.Add item
                    if canSkip streamState item then skipCount <- skipCount + 1
                    else
                        count <- count + 1
                        reqs.[item.stream] <- item.index+1L
                progressState.AppendBatch((epoch,checkpoint),reqs)
                work.Enqueue(Added (reqs.Count,skipCount,count))
            | AddStream streamSpan ->
                streams.Add(streamSpan,false) |> ignore
                work.Enqueue(Added (1,streamSpan.span.events.Length))
            | Added _  | ProgressResult _ ->
                ()
            | Result _ as r ->
                handleResult (streams, progressState, batches) r
            
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
                    dispatcher.Enqueue(project batch)
            // 4. Periodically emit status info
            let busy = processorDop - dispatcher.Capacity
            stats.TryDump(busy,processorDop,streams)
            do! Async.Sleep sleepIntervalMs }
    static member Start<'R>(stats, maxPendingBatches, processorDop, project, handleResult) =
        let instance = new Coordinator<'R>(maxPendingBatches, processorDop, project, handleResult)
        Async.Start <| instance.Pump(stats)
        instance
    static member Start(log, maxPendingBatches, processorDop, project : StreamSpan -> Async<int>, statsInterval) =
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
        let stats = Stats(log, maxPendingBatches, statsInterval)
        Coordinator<int64>.Start(stats, maxPendingBatches, processorDop, project, handleResult)

    member __.Submit(epoch, markBatchCompleted, events) = async {
        let! _ = batches.Await()
        Add (epoch, markBatchCompleted, Array.ofSeq events) |> work.Enqueue
        return maxPendingBatches-batches.CurrentCount,maxPendingBatches }

    member __.Submit(streamSpan) =
        AddStream streamSpan |> work.Enqueue

    member __.Stop() =
        cts.Cancel()