﻿module Jet.Projection.Ingestion

open Serilog
open System
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic

type FSharp.Control.Async with
    static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
        Async.FromContinuations <| fun (k,ek,_) ->
            task.ContinueWith (fun (t:Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                    else ek e
                elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                elif t.IsCompleted then k t.Result
                else ek(Exception "invalid Task state!"))
            |> ignore
    static member AwaitTaskCorrect (task : Task) : Async<unit> =
        Async.FromContinuations <| fun (k,ek,_) ->
            task.ContinueWith (fun (t:Task) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                    else ek e
                elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                elif t.IsCompleted then k ()
                else ek(Exception "invalid Task state!"))
            |> ignore

/// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
let intervalCheck (period : TimeSpan) =
    let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
    fun () ->
        let due = timer.ElapsedMilliseconds > max
        if due then timer.Restart()
        due

[<NoComparison; NoEquality>]        
type private InternalMessage =
    /// Confirmed completion of a batch
    | Validated of epoch: int64
    /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
    | ProgressResult of Choice<int64,exn>
    /// Internal message for stats purposes
    | Added of streams: int * events: int

type private Stats(log : ILogger, statsInterval : TimeSpan) =
    let mutable validatedEpoch, comittedEpoch : int64 option * int64 option = None, None
    let progCommitFails, progCommits = ref 0, ref 0 
    let cycles, batchesPended, streamsPended, eventsPended = ref 0, ref 0, ref 0, ref 0
    let statsDue = intervalCheck statsInterval
    let dumpStats (activeReads, maxReads) =
        log.Information("Buffering Cycles {cycles} Ingested {batches} ({streams:n0}s {events:n0}e)", !cycles, !batchesPended, !streamsPended, !eventsPended)
        cycles := 0; batchesPended := 0; streamsPended := 0; eventsPended := 0
        if !progCommitFails <> 0 || !progCommits <> 0 then
            match comittedEpoch with
            | None ->
                log.Error("Uncommitted {activeReads}/{maxReads} @ {validated}; writing failing: {failures} failures ({commits} successful commits)",
                        activeReads, maxReads, Option.toNullable validatedEpoch, !progCommitFails, !progCommits)
            | Some committed when !progCommitFails <> 0 ->
                log.Warning("Uncommitted {activeReads}/{maxReads} @ {validated} (committed: {committed}, {commits} commits, {failures} failures)",
                        activeReads, maxReads, Option.toNullable validatedEpoch, committed, !progCommits, !progCommitFails)
            | Some committed ->
                log.Information("Uncommitted {activeReads}/{maxReads} @ {validated} (committed: {committed}, {commits} commits)",
                        activeReads, maxReads, Option.toNullable validatedEpoch, committed, !progCommits)
            progCommits := 0; progCommitFails := 0
        else
            log.Information("Uncommitted {activeReads}/{maxReads} @ {validated} (committed: {committed})",
                    activeReads, maxReads, Option.toNullable validatedEpoch, Option.toNullable comittedEpoch)
    member __.Handle : InternalMessage -> unit = function
        | Validated epoch ->
            validatedEpoch <- Some epoch
        | ProgressResult (Choice1Of2 epoch) ->
            incr progCommits
            comittedEpoch <- Some epoch
        | ProgressResult (Choice2Of2 (_exn : exn)) ->
            incr progCommitFails
        | Added (streams,events) ->
            incr batchesPended
            streamsPended := !streamsPended + streams
            eventsPended := !eventsPended + events
    member __.TryDump(readState) =
        incr cycles
        let due = statsDue ()
        if due then dumpStats readState
        due

type Sem(max) =
    let inner = new SemaphoreSlim(max)
    member __.Await(ct : CancellationToken) = inner.WaitAsync(ct) |> Async.AwaitTaskCorrect
    //member __.TryTake() = inner.Wait 0
    member __.Release() = inner.Release() |> ignore
    member __.State = max-inner.CurrentCount,max

/// Buffers items read from a range, unpacking them out of band from the reading so that can overlap
/// On completion of the unpacking, they get submitted onward to the Submitter which will buffer them for us
type Ingester<'Items,'Batch> private
    (   log : ILogger, stats : Stats, maxRead, sleepInterval : TimeSpan,
        makeBatch : (unit->unit) -> 'Items -> ('Batch * (int * int)),
        submit : 'Batch -> unit,
        cts : CancellationTokenSource) =
    let sleepInterval = int sleepInterval.TotalMilliseconds
    let maxRead = Sem maxRead
    let incoming = new ConcurrentQueue<_>()
    let messages = new ConcurrentQueue<InternalMessage>()
    let tryDequeue (x : ConcurrentQueue<_>) =
        let mutable tmp = Unchecked.defaultof<_>
        if x.TryDequeue &tmp then Some tmp
        else None
    let progressWriter = Progress.Writer<_>()
    let rec tryIncoming () =
        match tryDequeue incoming with
        | None -> false
        | Some (epoch,checkpoint,items) ->
            let markCompleted () =
                maxRead.Release()
                messages.Enqueue (Validated epoch)
                progressWriter.Post(epoch,checkpoint)
            let batch,(streamCount, itemCount) = makeBatch markCompleted items
            submit batch
            messages.Enqueue(Added (streamCount,itemCount))
            true
    let rec tryHandle () =
        match tryDequeue messages with
        | None -> false
        | Some x ->
            stats.Handle x
            true

    member private __.Pump() = async {
        let! ct = Async.CancellationToken
        use _ = progressWriter.Result.Subscribe(ProgressResult >> messages.Enqueue)
        Async.Start(progressWriter.Pump(), ct)
        while not ct.IsCancellationRequested do
            // arguably the impl should be submitting while unpacking but
            // - maintaining consistency between incoming order and submit order is required
            // - in general maxRead will be double maxSubmit so this will only be relevant in catchup situations
            try let worked = tryHandle () || tryIncoming () || stats.TryDump(maxRead.State)
                if not worked then do! Async.Sleep sleepInterval
            with e -> log.Error(e, "Ingester exception") }
        
    /// Starts an independent Task which handles
    /// a) `unpack`ing of `incoming` items
    /// b) `submit`ting them onward (assuming there is capacity within the `readLimit`)
    static member Start<'Item>(log, maxRead, makeBatch, submit, ?statsInterval, ?sleepInterval) =
        let maxWait, statsInterval = defaultArg sleepInterval (TimeSpan.FromMilliseconds 5.), defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let cts = new CancellationTokenSource()
        let stats = Stats(log, statsInterval)
        let instance = Ingester<_,_>(log, stats, maxRead, maxWait, makeBatch, submit, cts)
        Async.Start(instance.Pump(), cts.Token)
        instance

    /// Submits a batch as read for unpacking and submission; will only return after the in-flight reads drops below the limit
    /// Returns (reads in flight,maximum reads in flight)
    member __.Submit(epoch, checkpoint, items) = async {
        // If we've read it, feed it into the queue for unpacking
        incoming.Enqueue (epoch, checkpoint, items)
        // ... but we might hold off on yielding if we're at capacity
        do! maxRead.Await(cts.Token)
        return maxRead.State }

    /// As range assignments get revoked, a user is expected to `Stop `the active processing thread for the Ingester before releasing references to it
    member __.Stop() = cts.Cancel()

type StreamsIngester =
    static member Start(log, maxRead, submit, ?statsInterval, ?sleepInterval) =
        let makeBatch onCompletion (partitionId,items : StreamItem seq) =
            let items = Array.ofSeq items
            let streams = HashSet(seq { for x in items -> x.stream })
            let batch : Submission.Batch<_> = { partitionId = partitionId; onCompletion = onCompletion; messages = items }
            batch,(streams.Count,items.Length)
        Ingester<int*StreamItem seq,Submission.Batch<StreamItem>>.Start(log, maxRead, makeBatch, submit, ?statsInterval = statsInterval, ?sleepInterval = sleepInterval)