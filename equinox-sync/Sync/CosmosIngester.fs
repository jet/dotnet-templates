module Equinox.Cosmos.Projection.CosmosIngester

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Equinox.Projection.Scheduling
open Equinox.Projection2
open Equinox.Projection2.Scheduling
open Equinox.Projection.State
open Serilog
open System.Threading

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

type Stats(log : ILogger, statsInterval) =
    inherit Stats<(int*int)*Writer.Result>(log, statsInterval)
    let resultOk, resultDup, resultPartialDup, resultPrefix, resultExnOther = ref 0, ref 0, ref 0, ref 0, ref 0
    let rateLimited, timedOut, tooLarge, malformed = ref 0, ref 0, ref 0, ref 0
    let mutable events, bytes = 0, 0L
    let badCats = CatStats()

    override __.DumpExtraStats() =
        let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix
        log.Information("Completed {completed:n0} {events:n0}e {mb:n0}MB ({ok:n0} ok {dup:n0} redundant {partial:n0} partial {prefix:n0} waiting)",
            results, events, mb bytes, !resultOk, !resultDup, !resultPartialDup, !resultPrefix)
        resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; events <- 0; bytes <- 0L
        if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 then
            log.Warning("Exceptions {rateLimited:n0} rate-limited, {timedOut:n0} timed out, {tooLarge} too large, {malformed} malformed, {other} other",
                !rateLimited, !timedOut, !tooLarge, !malformed, !resultExnOther)
            rateLimited := 0; timedOut := 0; tooLarge := 0; malformed := 0; resultExnOther := 0
            if badCats.Any then log.Error("Malformed categories {@badCats}", badCats.StatsDescending); badCats.Clear()
        Equinox.Cosmos.Store.Log.InternalMetrics.dump log

    override __.Handle message =
        base.Handle message
        match message with
        | Merge _
        | Added _ -> ()
        | Result (_stream, Choice1Of2 ((es,bs),r)) ->
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

let start (log : Serilog.ILogger, cosmosContexts : _ [], maxWriters, statsInterval) =
    let cosmosPayloadLimit = 2 * 1024 * 1024 - (*fudge*)4096
    let cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + (x.EventType.Length * 2) + 96
    let writerResultLog = log.ForContext<Writer.Result>()
    let trim (_currentWritePos : int64 option, batch : StreamSpan) =
        let mutable count, countBudget, bytesBudget = 0, 4096, cosmosPayloadLimit
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
    let projectionAndCosmosStats = Stats(log.ForContext<Stats>(), statsInterval)
    Engine<(int*int)*Writer.Result>.Start(projectionAndCosmosStats, maxWriters, attemptWrite, interpretWriteResultProgress)