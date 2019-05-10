﻿module Equinox.Cosmos.Projection.CosmosIngester

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Equinox.Projection.Scheduling
open Equinox.Projection2
open Equinox.Projection2.Scheduling
open Equinox.Projection.State
open Serilog
open System.Threading
open System.Collections.Generic

[<AutoOpen>]
module Writer =
    type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

    type [<NoComparison>] Result =
        | Ok of updatedPos: int64
        | Duplicate of updatedPos: int64
        | PartialDuplicate of overage: Span
        | PrefixMissing of batch: Span * writePos: int64
    let logTo (log: ILogger) (res : string * Choice<(int*int)*Result,exn>) =
        match res with
        | stream, (Choice1Of2 (_, Ok pos)) ->
            log.Information("Wrote     {stream} up to {pos}", stream, pos)
        | stream, (Choice1Of2 (_, Duplicate updatedPos)) ->
            log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
        | stream, (Choice1Of2 (_, PartialDuplicate overage)) ->
            log.Information("Requeing  {stream} {pos} ({count} events)", stream, overage.index, overage.events.Length)
        | stream, (Choice1Of2 (_, PrefixMissing (batch,pos))) ->
            log.Information("Waiting   {stream} missing {gap} events ({count} events @ {pos})", stream, batch.index-pos, batch.events.Length, batch.index)
        | stream, Choice2Of2 exn ->
            log.Warning(exn,"Writing   {stream} failed, retrying", stream)

    let write (log : ILogger) (ctx : CosmosContext) ({ stream = s; span = { index = i; events = e}} as batch) = async {
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

type Stats(log : ILogger, statsInterval, statesInterval) =
    inherit Stats<(int*int)*Writer.Result>(log, statsInterval, statesInterval)
    let okStreams, resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = HashSet(), ref 0, ref 0, ref 0, ref 0, ref 0
    let badCats, failStreams, rateLimited, timedOut, tooLarge, malformed = CatStats(), HashSet(), ref 0, ref 0, ref 0, ref 0
    let rlStreams, toStreams, tlStreams, mfStreams, oStreams = HashSet(), HashSet(), HashSet(), HashSet(), HashSet()
    let mutable events, bytes = 0, 0L

    override __.DumpExtraStats() =
        let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix
        log.Information("Completed {completed:n0}r {streams:n0}s {events:n0}e {mb:n0}MB ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
            results, okStreams.Count, events, mb bytes, !resultOk, !resultDup, !resultPartialDup, !resultPrefix)
        okStreams.Clear(); resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; events <- 0; bytes <- 0L
        if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 || !resultExnOther <> 0 then
            log.Warning("Failures {streams:n0}s Rate-limited {rateLimited:n0}r {rlStreams:n0}s Timed out {toCount:n0}r {toStreams:n0}s Other {other:n0} {@oStreams}",
                failStreams.Count, !rateLimited, rlStreams.Count, !timedOut, toStreams.Count, !resultExnOther, oStreams)
            rateLimited := 0; timedOut := 0; resultExnOther := 0; failStreams.Clear(); rlStreams.Clear(); toStreams.Clear(); oStreams.Clear()
        if badCats.Any then
            log.Warning("Malformed cats {@badCats} Too large {tooLarge:n0} {@tlStreams} Malformed {malformed:n0} {@mfStreams}",
                badCats.StatsDescending, !tooLarge, tlStreams, !malformed, mfStreams)
            badCats.Clear(); tooLarge := 0; malformed := 0; tlStreams.Clear(); mfStreams.Clear()
        Equinox.Cosmos.Store.Log.InternalMetrics.dump log

    override __.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        let inline bads x (set:HashSet<_>) = badCats.Ingest(category x); adds x set
        base.Handle message
        match message with
        | Merge _ | Added _ -> () // Processed by standard logging already; we have nothing to add
        | Result (stream, Choice1Of2 ((es,bs),r)) ->
            adds stream okStreams
            events <- events + es
            bytes <- bytes + int64 bs
            match r with
            | Writer.Result.Ok _ -> incr resultOk
            | Writer.Result.Duplicate _ -> incr resultDup
            | Writer.Result.PartialDuplicate _ -> incr resultPartialDup
            | Writer.Result.PrefixMissing _ -> incr resultPrefix
        | Result (stream, Choice2Of2 exn) ->
            adds stream failStreams
            match Writer.classify exn with
            | ResultKind.RateLimited -> adds stream rlStreams; incr rateLimited
            | ResultKind.TimedOut -> adds stream toStreams; incr timedOut
            | ResultKind.TooLarge -> bads stream tlStreams; incr tooLarge
            | ResultKind.Malformed -> bads stream mfStreams; incr malformed
            | ResultKind.Other -> bads stream oStreams; incr resultExnOther

let start (log : Serilog.ILogger, cosmosContexts : _ [], maxWriters, (statsInterval, statesInterval)) =
    let cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + (x.EventType.Length * 2) + 96
    let writerResultLog = log.ForContext<Writer.Result>()
    let trim (_currentWritePos : int64 option, batch : StreamSpan) =
        let mutable countBudget, bytesBudget = 16384, 512 * 1024 - (*fudge*)4096
        let mutable count = 0
        let withinLimits (y : Equinox.Codec.IEvent<byte[]>) =
            count <- count + 1
            countBudget <- countBudget - 1
            bytesBudget <- bytesBudget - cosmosPayloadBytes y
            // always send at least one event in order to surface the problem and have the stream marked malformed
            count = 1 || (countBudget >= 0 && bytesBudget >= 0)
        { stream = batch.stream; span = { index = batch.span.index; events =  batch.span.events |> Array.takeWhile withinLimits } }
    let mutable robin = 0
    let attemptWrite batch = async {
        let trimmed = trim batch
        let index = Interlocked.Increment(&robin) % cosmosContexts.Length
        let selectedConnection = cosmosContexts.[index]
        try let! res = Writer.write log selectedConnection trimmed
            let stats = trimmed.span.events.Length, trimmed.span.events |> Seq.sumBy cosmosPayloadBytes
            return Choice1Of2 (stats,res)
        with e -> return Choice2Of2 e }
    let interpretWriteResultProgress (streams: Scheduling.StreamStates) stream res =
        let applyResultToStreamState = function
            | Choice1Of2 (_stats, Writer.Ok pos) ->                       streams.InternalUpdate stream pos null
            | Choice1Of2 (_stats, Writer.Duplicate pos) ->                streams.InternalUpdate stream pos null
            | Choice1Of2 (_stats, Writer.PartialDuplicate overage) ->     streams.InternalUpdate stream overage.index [|overage|]
            | Choice1Of2 (_stats, Writer.PrefixMissing (overage,pos)) ->  streams.InternalUpdate stream pos [|overage|]
            | Choice2Of2 exn ->
                let malformed = Writer.classify exn |> Writer.isMalformed
                streams.SetMalformed(stream,malformed)
        let _stream, { write = wp } = applyResultToStreamState res
        Writer.logTo writerResultLog (stream,res)
        wp
    let projectionAndCosmosStats = Stats(log.ForContext<Stats>(), statsInterval, statesInterval)
    Engine<(int*int)*Writer.Result>.Start(projectionAndCosmosStats, maxWriters, attemptWrite, interpretWriteResultProgress)