﻿module Equinox.Projection.Cosmos.Ingestion

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Equinox.Projection.Engine
open Equinox.Projection.State
open Serilog
open System
open System.Threading

let cosmosPayloadLimit = 2 * 1024 * 1024 - (*fudge*)4096
let cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + 96

type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

module Writer =
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

    //member __.TryGap() : (string*int64*int) option =
    //    let rec aux () =
    //        match gap |> Queue.tryDequeue with
    //        | None -> None
    //        | Some stream ->
            
    //        match states.[stream].TryGap() with
    //        | Some (pos,count) -> Some (stream,pos,int count)
    //        | None -> aux ()
    //    aux ()

type CosmosStats(log : ILogger, maxPendingBatches, statsInterval) =
    inherit Stats<(int*int)*Writer.Result>(log, maxPendingBatches, statsInterval)
    let resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = ref 0, ref 0, ref 0, ref 0, ref 0
    let rateLimited, timedOut, tooLarge, malformed = ref 0, ref 0, ref 0, ref 0
    let mutable events, bytes = 0, 0L
    let badCats = CatStats()

    override __.DumpExtraStats() =
        let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix
        log.Information("Requests {completed:n0} {events:n0}e {mb:n0}MB ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
            results, events, mb bytes, !resultOk, !resultDup, !resultPartialDup, !resultPrefix)
        resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0
        if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 then
            log.Warning("Exceptions {rateLimited:n0} rate-limited, {timedOut:n0} timed out, {tooLarge} too large, {malformed} malformed, {other} other",
                !rateLimited, !timedOut, !tooLarge, !malformed, !resultExnOther)
            rateLimited := 0; timedOut := 0; tooLarge := 0; malformed := 0; resultExnOther := 0; events <- 0; bytes <- 0L
            if badCats.Any then log.Error("Malformed categories {badCats}", badCats.StatsDescending); badCats.Clear()
        Metrics.dumpRuStats statsInterval log

    override __.Handle message =
        base.Handle message
        match message with
        | Message.Result (_stream, Choice1Of2 ((es,bs),r)) ->
            events <- events + es
            bytes <- bytes + int64 bs
            match r with
            | Writer.Result.Ok _ -> incr resultOk
            | Writer.Result.Duplicate _ -> incr resultDup
            | Writer.Result.PartialDuplicate _ -> incr resultPartialDup
            | Writer.Result.PrefixMissing _ -> incr resultPrefix
        | Result (stream, Choice2Of2 exn) ->
            match Writer.classify exn with
            | ResultKind.Other -> incr resultExnOther
            | ResultKind.RateLimited -> incr rateLimited
            | ResultKind.TooLarge -> category stream |> badCats.Ingest; incr tooLarge
            | ResultKind.Malformed -> category stream |> badCats.Ingest; incr malformed
            | ResultKind.TimedOut -> incr timedOut
        | Add _ | AddStream _ | Added _ | ProgressResult _ -> ()

module CosmosIngestionCoordinator =
    let create (log : Serilog.ILogger, cosmosContext, maxWriters, maxPendingBatches, statsInterval) =
        let writerResultLog = log.ForContext<Writer.Result>()
        let trim (writePos : int64 option, batch : StreamSpan) =
            let mutable bytesBudget = cosmosPayloadLimit
            let mutable count = 0
            let max2MbMax100EventsMax10EventsFirstTranche (y : Equinox.Codec.IEvent<byte[]>) =
                bytesBudget <- bytesBudget - cosmosPayloadBytes y
                count <- count + 1
                // Reduce the item count when we don't yet know the write position
                count <= (if Option.isNone writePos then 100 else 4096) && (bytesBudget >= 0 || count = 1)
            { stream = batch.stream; span = { index = batch.span.index; events = batch.span.events |> Array.takeWhile max2MbMax100EventsMax10EventsFirstTranche } }
        let project batch = async {
            let trimmed = trim batch
            try let! res = Writer.write log cosmosContext trimmed
                let ctx = trimmed.span.events.Length, trimmed.span.events |> Seq.sumBy cosmosPayloadBytes
                return trimmed.stream, Choice1Of2 (ctx,res)
            with e -> return trimmed.stream, Choice2Of2 e }
        let handleResult (streams: StreamStates, progressState : ProgressState<_>,  batches: SemaphoreSlim) res =
            let applyResultToStreamState = function
                | stream, (Choice1Of2 (ctx, Writer.Ok pos)) ->
                    Some ctx,streams.InternalUpdate stream pos null
                | stream, (Choice1Of2 (ctx, Writer.Duplicate pos)) ->
                    Some ctx,streams.InternalUpdate stream pos null
                | stream, (Choice1Of2 (ctx, Writer.PartialDuplicate overage)) ->
                    Some ctx,streams.InternalUpdate stream overage.index [|overage|]
                | stream, (Choice1Of2 (ctx, Writer.PrefixMissing (overage,pos))) ->
                    Some ctx,streams.InternalUpdate stream pos [|overage|]
                | stream, (Choice2Of2 exn) ->
                    let malformed = Writer.classify exn |> Writer.isMalformed
                    None,streams.SetMalformed(stream,malformed)
            match res with
            | Message.Result (s,r) ->
                let _ctx,(stream,updatedState) = applyResultToStreamState (s,r)
                match updatedState.write with
                | Some wp ->
                    let closedBatches = progressState.MarkStreamProgress(stream, wp)
                    if closedBatches > 0 then
                        batches.Release(closedBatches) |> ignore
                    streams.MarkCompleted(stream,wp)
                | None ->
                    streams.MarkFailed stream
                Writer.logTo writerResultLog (s,r)
            | _ -> ()
        let stats = CosmosStats(log, maxPendingBatches, statsInterval)
        Coordinator<(int*int)*Writer.Result>.Start(stats, maxPendingBatches, maxWriters, project, handleResult)