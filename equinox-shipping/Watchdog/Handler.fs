module Shipping.Watchdog.Handler

open System

[<RequireQualifiedAccess>]
type Outcome = Completed | Deferred | Resolved of successfully : bool

/// Gathers stats based on the outcome of each Span processed, periodically including them in the Sink summaries
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Projector.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable completed, deferred, failed, succeeded = 0, 0, 0, 0

    override __.HandleOk res = res |> function
        | Outcome.Completed -> completed <- completed + 1
        | Outcome.Deferred -> deferred <- deferred + 1
        | Outcome.Resolved successfully -> if successfully then succeeded <- succeeded + 1 else failed <- failed + 1

    override __.DumpStats () =
        if completed <> 0 || deferred <> 0 || failed <> 0 || succeeded <> 0 then
            log.Information(" Completed {completed} Deferred {deferred} Failed {failed} Succeeded {succeeded}", completed, deferred, failed, succeeded)
            completed <- 0; deferred <- 0; failed <- 0; succeeded <- 0

open Shipping.Domain

let tryHandle
        (processingTimeout : TimeSpan)
        (driveTransaction : Shipping.Domain.TransactionId -> Async<bool>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<Outcome> = async {
    let processingStuckCutoff = let now = DateTimeOffset.UtcNow in now.Add(-processingTimeout)
    match stream, span.events with
    | TransactionWatchdog.Finalization.MatchStatus (transId, state) ->
        match TransactionWatchdog.toStatus processingStuckCutoff state with
        | TransactionWatchdog.Complete ->
            return Outcome.Completed
        | TransactionWatchdog.Active ->
            // We don't want to be warming the data center for no purpose; visiting every second is not too expensive
            do! Async.Sleep 1000 // ms
            return Outcome.Deferred
        | TransactionWatchdog.Stuck ->
            let! success = driveTransaction transId
            return Outcome.Resolved success
    | other -> return failwithf "Span from unexpected category %A" other }

let handleStreamEvents tryHandle (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64*Outcome> = async {
    match! tryHandle (stream, span) with
    // if we're deferring, we need to retain the events (all of them, as the time of the first one is salient)
    | Outcome.Deferred as r -> return span.index, r
    // if it's now complete (either organically, or with our help), we can release all the events pertaining to this transaction
    | Outcome.Completed | Outcome.Resolved _ as r -> return span.index + span.events.LongLength, r }
