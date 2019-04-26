module Equinox.Projection.Cosmos.CosmosIngester

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Equinox.Projection.Coordination
open Equinox.Projection.State
open Serilog

let cosmosPayloadLimit = 2 * 1024 * 1024 - (*fudge*)4096
let cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + 96

type [<RequireQualifiedAccess>] ResultKind = TimedOut | RateLimited | TooLarge | Malformed | Other

module Writer =
    type [<NoComparison>] Result =
        | Ok of updatedPos: int64
        | Duplicate of updatedPos: int64
        | PartialDuplicate of overage: Span
        | PrefixMissing of batch: Span * writePos: int64
    let logTo (log: ILogger) (res : string * Choice<Result,exn>) =
        match res with
        | stream, (Choice1Of2 (Ok pos)) ->
            log.Information("Wrote     {stream} up to {pos}", stream, pos)
        | stream, (Choice1Of2 (Duplicate updatedPos)) ->
            log.Information("Ignored   {stream} (synced up to {pos})", stream, updatedPos)
        | stream, (Choice1Of2 (PartialDuplicate overage)) ->
            log.Information("Requeing  {stream} {pos} ({count} events)", stream, overage.index, overage.events.Length)
        | stream, (Choice1Of2 (PrefixMissing (batch,pos))) ->
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
    //member __.TryReady(isBusy) =
    //    let blocked = ResizeArray()
    //    let rec aux () =
    //        match dirty |> Queue.tryDequeue with
    //        | None -> None
    //        | Some stream ->

    //        match states.[stream] with
    //        | state when state.IsReady ->
    //            if (not << isBusy) stream then 
    //                let h = state.queue |> Array.head
                
    //                let mutable bytesBudget = cosmosPayloadLimit
    //                let mutable count = 0 
    //                let max2MbMax100EventsMax10EventsFirstTranche (y : Equinox.Codec.IEvent<byte[]>) =
    //                    bytesBudget <- bytesBudget - cosmosPayloadBytes y
    //                    count <- count + 1
    //                    // Reduce the item count when we don't yet know the write position
    //                    count <= (if Option.isNone state.write then 10 else 4096) && (bytesBudget >= 0 || count = 1)
    //                Some { stream = stream; span = { index = h.index; events = h.events |> Array.takeWhile max2MbMax100EventsMax10EventsFirstTranche } }
    //            else
    //                blocked.Add(stream) |> ignore
    //                aux ()
    //        | _ -> aux ()
    //    let res = aux ()
    //    for x in blocked do markDirty x
    //    res

type CosmosStats (log : ILogger, maxPendingBatches, statsInterval) =
    inherit Stats<Writer.Result>(log, maxPendingBatches, statsInterval)
    let resultOk, resultDup, resultPartialDup, resultPrefix, resultExn = ref 0, ref 0, ref 0, ref 0, ref 0
    let rateLimited, timedOut, tooLarge, malformed = ref 0, ref 0, ref 0, ref 0
    let badCats = CatStats()

    override __.DumpExtraStats() =
        let results = !resultOk + !resultDup + !resultPartialDup + !resultPrefix + !resultExn
        log.Information("Wrote {completed} ({ok} ok {dup} redundant {partial} partial {prefix} Missing {exns} Exns)",
            results, !resultOk, !resultDup, !resultPartialDup, !resultPrefix, !resultExn)
        resultOk := 0; resultDup := 0; resultPartialDup := 0; resultPrefix := 0; resultExn := 0;
        if !rateLimited <> 0 || !timedOut <> 0 || !tooLarge <> 0 || !malformed <> 0 then
            log.Warning("Exceptions {rateLimited} rate-limited, {timedOut} timed out, {tooLarge} too large, {malformed} malformed",
                !rateLimited, !timedOut, !tooLarge, !malformed)
            rateLimited := 0; timedOut := 0; tooLarge := 0; malformed := 0 
            if badCats.Any then log.Error("Malformed categories {badCats}", badCats.StatsDescending); badCats.Clear()
        Metrics.dumpRuStats statsInterval log

    override __.Handle message =
        base.Handle message
        match message with
        | Message.Result (_stream, Choice1Of2 r) ->
            match r with
            | Writer.Result.Ok _ -> incr resultOk
            | Writer.Result.Duplicate _ -> incr resultDup
            | Writer.Result.PartialDuplicate _ -> incr resultPartialDup
            | Writer.Result.PrefixMissing _ -> incr resultPrefix
        | Result (stream, Choice2Of2 exn) ->
            match Writer.classify exn with
            | ResultKind.Other -> ()
            | ResultKind.RateLimited -> incr rateLimited
            | ResultKind.TooLarge -> category stream |> badCats.Ingest; incr tooLarge
            | ResultKind.Malformed -> category stream |> badCats.Ingest; incr malformed
            | ResultKind.TimedOut -> incr timedOut
        | Add _ | AddStream _ | Added _ | ProgressResult _ -> ()