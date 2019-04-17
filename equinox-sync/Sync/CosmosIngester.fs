module SyncTemplate.CosmosIngester

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Serilog
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

let arrayBytes (x:byte[]) = if x = null then 0 else x.Length
let private mb x = float x / 1024. / 1024.

let category (streamName : string) = streamName.Split([|'-'|],2).[0]

let cosmosPayloadLimit = 2 * 1024 * 1024 - 1024 (*fudge*) - 2048
let cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + 4 (* fudge*) + 128

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
            | Ok (stream, pos) ->           log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | Duplicate (stream, pos) ->    log.Information("Ignored   {stream} (synced up to {pos})", stream, pos)
            | PartialDuplicate overage ->   log.Information("Requeing  {stream} {pos} ({count} events)",
                                                                overage.stream, overage.span.index, overage.span.events.Length)
            | PrefixMissing (batch,pos) ->  log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})",
                                                                batch.stream, batch.span.index-pos, batch.span.events.Length, batch.span.index)
            | Exn (exn, batch) ->           log.Warning(exn,"Writing   {stream} failed, retrying {count} events ....", batch.stream, batch.span.events.Length)

    let write (log : ILogger) (ctx : CosmosContext) ({ stream = s; span = { index = i; events = e}} as batch) = async {
        let stream = ctx.CreateStream s
        log.Debug("Writing {s}@{i}x{n}",s,i,e.Length)
        try let! res = ctx.Sync(stream, { index = i; etag = None }, e)
            let ress =
                match res with
                | AppendResult.Ok pos -> Ok (s, pos.index)
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index with
                    | actual when actual < i -> PrefixMissing (batch, actual)
                    | actual when actual >= i + e.LongLength -> Duplicate (s, actual)
                    | actual -> PartialDuplicate { stream = s; span = { index = actual; events = e |> Array.skip (actual-i |> int) } }
            log.Debug("Result: {res}",ress)
            return ress
        with e -> return Exn (e, batch) }
    let (|TimedOutMessage|RateLimitedMessage|MalformedMessage|Other|) (e: exn) =
        match string e with
        | m when m.Contains "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
        | m when m.Contains "Microsoft.Azure.Documents.RequestRateTooLargeException" -> RateLimitedMessage
        | m when m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
             || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position" -> MalformedMessage
        | _ -> Other

type [<NoComparison>] StreamState = { isMalformed : bool; write: int64 option; queue: Span[] } with
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
    member __.Size =
        if __.queue = null then 0
        else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy (fun x -> arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length*2 + 16)

module StreamState =
    let (|NNA|) xs = if xs = null then Array.empty else xs
    module Span =
        let (|End|) x = x.index + if x.events = null then 0L else x.events.LongLength
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
        { write = writePos; queue = Span.merge (defaultArg writePos 0L) items; isMalformed = s1.isMalformed || s2.isMalformed}

/// Gathers stats relating to how many items of a given category have been observed
type CatStats() =
    let cats = Dictionary<string,int>()
    member __.Ingest(cat,?weight) = 
        let weight = defaultArg weight 1
        match cats.TryGetValue cat with
        | true, catCount -> cats.[cat] <- catCount + weight
        | false, _ -> cats.[cat] <- weight
    member __.Any = cats.Count <> 0
    member __.Clear() = cats.Clear()
    member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd

type ResultKind = TimedOut | RateLimited | Malformed | Ok

type StreamStates() =
    let states = Dictionary<string, StreamState>()
    let dirty = Queue()
    let markDirty stream = if (not << dirty.Contains) stream then dirty.Enqueue stream
    let gap = Queue()
    let markGap stream = if (not << gap.Contains) stream then gap.Enqueue stream
    
    let update stream (state : StreamState) =
        match states.TryGetValue stream with
        | false, _ ->
            states.Add(stream, state)
            markDirty stream
            stream, state
        | true, current ->
            let updated = StreamState.combine current state
            states.[stream] <- updated
            if updated.IsReady then
                //if (not << dirty.Contains) stream then Log.Information("Dirty {s} {w} {sz}", (stream : string), updated.write, updated.Size)
                markDirty stream
            //elif Option.isNone state.write then
            //    Log.Information("None {s} {w} {sz}", stream, updated.write, updated.Size)
            stream, updated
    let updateWritePos stream pos isMalformed span =
        update stream { write = pos; queue = span; isMalformed = isMalformed }

    member __.Add(item: Batch, ?isMalformed) = updateWritePos item.stream None (defaultArg isMalformed false) [|item.span|]
    member __.TryGetStreamWritePos stream = match states.TryGetValue stream with true, value -> value.write | _ -> None
    member __.HandleWriteResult = function
        | Writer.Result.Ok (stream, pos) -> updateWritePos stream (Some pos) false null, Ok
        | Writer.Result.Duplicate (stream, pos) -> updateWritePos stream (Some pos) false null, Ok
        | Writer.Result.PartialDuplicate overage -> updateWritePos overage.stream (Some overage.span.index) false [|overage.span|], Ok
        | Writer.Result.PrefixMissing (overage,pos) ->
            markGap overage.stream
            updateWritePos overage.stream (Some pos) false [|overage.span|], Ok
        | Writer.Result.Exn (exn, batch) ->
            let r, malformed = 
                match exn with
                | Writer.RateLimitedMessage -> RateLimited, false
                | Writer.TimedOutMessage -> TimedOut, false
                | Writer.MalformedMessage -> Malformed, true
                | Writer.Other -> Ok, false
            __.Add(batch, malformed), r
    member __.TryGap() : (string*int64*int) option =
        let rec aux () =
            match gap |> Queue.tryDequeue with
            | None -> None
            | Some stream ->
            
            match states.[stream].TryGap() with
            | None -> aux ()
            | Some (pos,count) -> Some (stream,pos,int count)
        aux ()
    member __.TryReady(isBusy) =
        let blocked = ResizeArray()
        let rec aux () =
            match dirty |> Queue.tryDequeue with
            | None -> None
            | Some stream ->

            match states.[stream] with
            | s when not s.IsReady -> aux ()
            | state ->
                if isBusy stream then 
                    blocked.Add(stream) |> ignore
                    aux ()
                else
                    let h = state.queue |> Array.head
                
                    let mutable bytesBudget = cosmosPayloadLimit
                    let mutable count = 0 
                    let max2MbMax100EventsMax10EventsFirstTranche (y : Equinox.Codec.IEvent<byte[]>) =
                        bytesBudget <- bytesBudget - cosmosPayloadBytes y
                        count <- count + 1
                        // Reduce the item count when we don't yet know the write position
                        count <= (if Option.isNone state.write then 10 else 4096) && (bytesBudget >= 0 || count = 1)
                    Some { stream = stream; span = { index = h.index; events = h.events |> Array.takeWhile max2MbMax100EventsMax10EventsFirstTranche } }
        let res = aux ()
        for x in blocked do markDirty x
        res
    member __.Dump(log : ILogger) =
        let mutable synced, ready, waiting, malformed = 0, 0, 0, 0
        let mutable readyB, waitingB, malformedB = 0L, 0L, 0L
        let waitCats, readyCats, readyStreams = CatStats(), CatStats(), CatStats()
        for KeyValue (stream,state) in states do
            match int64 state.Size with
            | 0L ->
                synced <- synced + 1
            | sz when state.isMalformed ->
                malformed <- malformed + 1
                malformedB <- malformedB + sz
            | sz when state.IsReady ->
                readyCats.Ingest(category stream)
                readyStreams.Ingest(sprintf "%s@%A" stream state.write, int sz)
                ready <- ready + 1
                readyB <- readyB + sz
            | sz ->
                waitCats.Ingest(category stream)
                waiting <- waiting + 1
                waitingB <- waitingB + sz
        log.Information("Synced {synced} Dirty {dirty} Ready {ready}/{readyMb:n1}MB Awaiting prefix {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
            synced, dirty.Count, ready, mb readyB, waiting, mb waitingB, malformed, mb malformedB)
        if waitCats.Any then log.Warning("Waiting {waitCats}", waitCats.StatsDescending)
        if readyCats.Any then log.Information("Ready {readyCats} {readyStreams}", readyCats.StatsDescending, Seq.truncate 10 readyStreams.StatsDescending)

type RefCounted<'T> = { mutable refCount: int; value: 'T }

// via https://stackoverflow.com/a/31194647/11635
type SemaphorePool(gen : unit -> SemaphoreSlim) =
    let inners: Dictionary<string, RefCounted<SemaphoreSlim>> = Dictionary()

    let getOrCreateSlot key =
        lock inners <| fun () ->
            match inners.TryGetValue key with
            | true, inner ->
                inner.refCount <- inner.refCount + 1
                inner.value
            | false, _ ->
                let value = gen ()
                inners.[key] <- { refCount = 1; value = value }
                value
    let slotReleaseGuard key : System.IDisposable =
        { new System.IDisposable with
            member __.Dispose() =
                lock inners <| fun () ->
                    let item = inners.[key]
                    match item.refCount with
                    | 1 -> inners.Remove key |> ignore
                    | current -> item.refCount <- current - 1 }

    member __.ExecuteAsync(k,f) = async {
        let x = getOrCreateSlot k
        use _ = slotReleaseGuard k
        return! f x }

    member __.Execute(k,f) =
        let x = getOrCreateSlot k
        use _l = slotReleaseGuard k
        f x
        
 type Writers(write, maxDop) =
    let work = ConcurrentQueue()
    let result = Event<Writer.Result>()
    let locks = SemaphorePool(fun () -> new SemaphoreSlim 1)
    [<CLIEvent>] member __.Result = result.Publish
    member __.Enqueue item = work.Enqueue item
    member __.HasCapacity = work.Count < maxDop
    member __.IsStreamBusy stream =
        let checkBusy (x : SemaphoreSlim) = x.CurrentCount = 0
        locks.Execute(stream,checkBusy)
    member __.Pump() = async {
        let dop = new SemaphoreSlim(maxDop)
        let dispatch item = async { let! res = write item in result.Trigger res } |> dop.Throttle
        let! ct = Async.CancellationToken
        while not ct.IsCancellationRequested do
            match work.TryDequeue() with
            | true, item ->
                let holdStreamWhileDispatching (streamLock : SemaphoreSlim) = async { do! dispatch item |> streamLock.Throttle }
                do! locks.ExecuteAsync(item.stream,holdStreamWhileDispatching)
            | _ -> do! Async.Sleep 100 }