module Fc.Watchdog.Handler

open System

[<RequireQualifiedAccess>]
type Outcome = Completed | Deferred | Nudged

type Stats(log, ?statsInterval, ?stateInterval) =
    inherit Propulsion.Streams.Projector.Stats<Outcome>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))

    let mutable completed, deferred, resolved = 0, 0, 0

    override __.HandleOk res = res |> function
        | Outcome.Completed -> completed <- completed + 1
        | Outcome.Deferred -> deferred <- deferred + 1
        | Outcome.Nudged -> resolved <- resolved + 1

    override __.DumpStats () =
        if completed <> 0 || deferred <> 0 then
            log.Information(" Completed {completed} Deferred {deferred} Resolved {resolved}", completed, deferred, resolved)
            completed <- 0; deferred <- 0; resolved <- 0

open Fc.Inventory.Transaction

let tryHandle driveTransaction (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<Outcome> = async {
    let processingStuckCutoff = let now = DateTimeOffset.UtcNow in now.AddSeconds -10.
    match stream, span.events with
    | Watchdog.Match (transId, state) ->
        match Watchdog.categorize processingStuckCutoff state with
        | Watchdog.Complete ->
            return Outcome.Completed
        | Watchdog.Active ->
            // Visiting every second is not too expensive; we don't want to be warming the data center for no purpose
            do! Async.Sleep 1000 // ms
            return Outcome.Deferred
        | Watchdog.Stuck ->
            do! driveTransaction transId
            return Outcome.Nudged
    | _ -> return failwith "Not a relevant event" }

let handleStreamEvents tryHandle (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64*Outcome> = async {
    match! tryHandle (stream, span) with
    // if we're deferring, we need to retain the events (all of them, as the time of the first one is salient)
    | Outcome.Deferred as r -> return span.index, r
    // if it's now complete (either organically, or with our help), we can release all the events pertaining to this transaction
    | Outcome.Completed | Outcome.Nudged as r -> return span.index + span.events.LongLength, r }
