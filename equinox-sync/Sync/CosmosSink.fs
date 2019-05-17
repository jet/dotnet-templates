module SyncTemplate.CosmosSink

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Equinox.Projection
open Equinox.Projection.Buffer
open Equinox.Projection.Scheduling
open Serilog
open System.Threading
open System.Collections.Generic

[<AutoOpen>]
module private Impl =
    let inline mb x = float x / 1024. / 1024.

[<AutoOpen>]
module Writer =
    type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

    type [<NoComparison>] Result =
        | Ok of updatedPos: int64
        | Duplicate of updatedPos: int64
        | PartialDuplicate of overage: Span
        | PrefixMissing of batch: Span * writePos: int64
    let logTo (log: ILogger) (res : string * Choice<(int*int)*Result,(int*int)*exn>) =
        match res with
        | stream, (Choice1Of2 (_, Ok pos)) ->
            log.Information("Wrote     {stream} up to {pos}", stream, pos)
        | stream, (Choice1Of2 (_, Duplicate updatedPos)) ->
            log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
        | stream, (Choice1Of2 (_, PartialDuplicate overage)) ->
            log.Information("Requeing  {stream} {pos} ({count} events)", stream, overage.index, overage.events.Length)
        | stream, (Choice1Of2 (_, PrefixMissing (batch,pos))) ->
            log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch.index-pos, batch.events.Length, batch.index)
        | stream, (Choice2Of2 (_, exn)) ->
            log.Warning(exn,"Writing   {stream} failed, retrying", stream)

    let write (log : ILogger) (ctx : CosmosContext) ({ stream = s; span = { index = i; events = e}} : StreamSpan as batch) = async {
        let stream = ctx.CreateStream s
        log.Debug("Writing {s}@{i}x{n}",s,i,e.Length)
        let! res = ctx.Sync(stream, { index = i; etag = None }, e)
        let ress =
            match res with
            | AppendResult.Ok pos -> Ok pos.index
            | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                match pos.index with
                | actual when actual < i -> PrefixMissing (batch.span, actual)
                | actual when actual >= i + e.LongLength -> Duplicate actual
                | actual -> PartialDuplicate { index = actual; events = e |> Array.skip (actual-i |> int) }
        log.Debug("Result: {res}",ress)
        return ress }
    let (|TimedOutMessage|RateLimitedMessage|TooLargeMessage|MalformedMessage|Other|) (e: exn) =
        match string e with
        | m when m.Contains "Microsoft.Azure.Documents.RequestTimeoutException" -> TimedOutMessage
        | m when m.Contains "Microsoft.Azure.Documents.RequestRateTooLargeException" -> RateLimitedMessage
        | m when m.Contains "Microsoft.Azure.Documents.RequestEntityTooLargeException" -> TooLargeMessage
        | m when m.Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
             || m.Contains "SyntaxError: JSON.parse Error: Invalid character at position" -> MalformedMessage
        | _ -> Other

    let classify = function
        | RateLimitedMessage -> ResultKind.RateLimited
        | TimedOutMessage -> ResultKind.TimedOut
        | TooLargeMessage -> ResultKind.TooLarge
        | MalformedMessage -> ResultKind.Malformed
        | Other -> ResultKind.Other
    let isMalformed = function
        | ResultKind.RateLimited | ResultKind.TimedOut | ResultKind.Other -> false
        | ResultKind.TooLarge | ResultKind.Malformed -> true

type Stats(log : ILogger, categorize, statsInterval, statesInterval) =
    inherit Scheduling.Stats<(int*int)*Writer.Result,(int*int)*exn>(log, statsInterval, statesInterval)
    let okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), ref 0, ref 0, ref 0, ref 0, ref 0
    let badCats, failStreams, rateLimited, timedOut, tooLarge, malformed = CatStats(), HashSet(), ref 0, ref 0, ref 0, ref 0
    let rlStreams, toStreams, tlStreams, mfStreams, oStreams = HashSet(), HashSet(), HashSet(), HashSet(), HashSet()
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

    override __.DumpExtraStats() =
        let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix
        log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
            mb okBytes, results, okStreams.Count, okEvents, !resultOk, !resultDup, !resultPartialDup, !resultPrefix)
        okStreams.Clear(); resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; okEvents <- 0; okBytes <- 0L
        if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 || badCats.Any then
            let fails = !rateLimited + !timedOut + !tooLarge + !malformed + !resultExnOther
            log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s",
                mb exnBytes, fails, failStreams.Count, exnEvents, !rateLimited, rlStreams.Count, !timedOut, toStreams.Count)
            rateLimited := 0; timedOut := 0; resultExnOther := 0; failStreams.Clear(); rlStreams.Clear(); toStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
        if badCats.Any then
            log.Warning("Malformed cats {@badCats} Too large {tooLarge:n0}r {@tlStreams} Malformed {malformed:n0}r {@mfStreams} Other {other:n0}r {@oStreams}",
                badCats.StatsDescending, !tooLarge, tlStreams, !malformed, mfStreams, !resultExnOther, oStreams)
            badCats.Clear(); tooLarge := 0; malformed := 0;  resultExnOther := 0; tlStreams.Clear(); mfStreams.Clear(); oStreams.Clear()
        Equinox.Cosmos.Store.Log.InternalMetrics.dump log

    override __.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        let inline bads x (set:HashSet<_>) = badCats.Ingest(categorize x); adds x set
        base.Handle message
        match message with
        | Merge _ | Added _ -> () // Processed by standard logging already; we have nothing to add
        | Result (stream, Choice1Of2 ((es,bs),r)) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            match r with
            | Writer.Result.Ok _ -> incr resultOk
            | Writer.Result.Duplicate _ -> incr resultDup
            | Writer.Result.PartialDuplicate _ -> incr resultPartialDup
            | Writer.Result.PrefixMissing _ -> incr resultPrefix
        | Result (stream, Choice2Of2 ((es,bs),exn)) ->
            adds stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            match Writer.classify exn with
            | ResultKind.RateLimited -> adds stream rlStreams; incr rateLimited
            | ResultKind.TimedOut -> adds stream toStreams; incr timedOut
            | ResultKind.TooLarge -> bads stream tlStreams; incr tooLarge
            | ResultKind.Malformed -> bads stream mfStreams; incr malformed
            | ResultKind.Other -> bads stream oStreams; incr resultExnOther

type Scheduler =
    static member Start(log : Serilog.ILogger, cosmosContexts : _ [], maxWriters, categorize, (statsInterval, statesInterval))
            : ISchedulingEngine =
        let writerResultLog = log.ForContext<Writer.Result>()
        let mutable robin = 0
        let attemptWrite (_writePos,batch) = async {
            let index = Interlocked.Increment(&robin) % cosmosContexts.Length
            let selectedConnection = cosmosContexts.[index]
            let maxEvents, maxBytes = 16384, 1024 * 1024 - (*fudge*)4096
            let stats, span' = Span.slice (maxEvents,maxBytes) batch.span
            let trimmed = { batch with span = span' }
            try let! res = Writer.write log selectedConnection trimmed
                return Choice1Of2 (stats,res)
            with e -> return Choice2Of2 (stats,e) }
        let interpretWriteResultProgress (streams: Scheduling.StreamStates) stream res =
            let applyResultToStreamState = function
                | Choice1Of2 (_stats, Writer.Ok pos) ->                       streams.InternalUpdate stream pos null
                | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.InternalUpdate stream pos null
                | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.InternalUpdate stream overage.index [|overage|]
                | Choice1Of2 (_stats, Writer.PrefixMissing (overage,pos)) ->  streams.InternalUpdate stream pos [|overage|]
                | Choice2Of2 (_stats, exn) ->
                    let malformed = Writer.classify exn |> Writer.isMalformed
                    streams.SetMalformed(stream,malformed)
            let _stream, { write = wp } = applyResultToStreamState res
            Writer.logTo writerResultLog (stream,res)
            wp
        let projectionAndCosmosStats = Stats(log.ForContext<Stats>(), categorize, statsInterval, statesInterval)
        Engine<(int*int)*Writer.Result,_>.Start(projectionAndCosmosStats, maxWriters, attemptWrite, interpretWriteResultProgress, fun s l -> s.Dump(l, categorize)) :> _