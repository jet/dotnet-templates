module Shipping.Watchdog.Handler

open System

[<RequireQualifiedAccess>]
type Outcome = Completed | Deferred | Resolved of successfully : bool

/// Gathers stats based on the outcome of each Span processed, periodically including them in the Sink summaries
type Stats(log, statsInterval, stateInterval, verboseStore, ?logExternalStats) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable completed, deferred, failed, succeeded = 0, 0, 0, 0
    member val StatsInterval = statsInterval
    member val StateInterval = stateInterval

    override _.HandleOk res = res |> function
        | Outcome.Completed -> completed <- completed + 1
        | Outcome.Deferred -> deferred <- deferred + 1
        | Outcome.Resolved successfully -> if successfully then succeeded <- succeeded + 1 else failed <- failed + 1
    override _.HandleExn(log, exn) =
        Exception.dump verboseStore log exn

    override _.DumpStats() =
        if completed <> 0 || deferred <> 0 || failed <> 0 || succeeded <> 0 then
            log.Information(" Completed {completed} Deferred {deferred} Failed {failed} Succeeded {succeeded}", completed, deferred, failed, succeeded)
            completed <- 0; deferred <- 0; failed <- 0; succeeded <- 0
        match logExternalStats with None -> () | Some f -> f Serilog.Log.Logger
        base.DumpStats()

open Shipping.Domain

let isReactionStream = function
    | FinalizationTransaction.StreamName _ -> true
    | _ -> false

let handle
        (processingTimeout : TimeSpan)
        (driveTransaction : Shipping.Domain.TransactionId -> Async<bool>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) = async {
    let processingStuckCutoff = let now = DateTimeOffset.UtcNow in now.Add(-processingTimeout)
    match stream, span.events with
    | TransactionWatchdog.Finalization.MatchStatus (transId, state) ->
        match TransactionWatchdog.toStatus processingStuckCutoff state with
        | TransactionWatchdog.Complete ->
            return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Completed
        | TransactionWatchdog.Active ->
            // We don't want to be warming the data center for no purpose; visiting every second is not too expensive
            do! Async.Sleep 1000 // ms
            return Propulsion.Streams.SpanResult.PartiallyProcessed 0, Outcome.Deferred
        | TransactionWatchdog.Stuck ->
            let! success = driveTransaction transId
            return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Resolved success
    | other ->
        return failwithf "Span from unexpected category %A" other }

type Config private () =
    
    static member CreateStats(log, statsInterval, stateInterval, ?storeVerbose, ?dump) =
        Stats(log, statsInterval, stateInterval, defaultArg storeVerbose false, ?logExternalStats=dump)

    static member private StartProjector(log : Serilog.ILogger, stats : Stats, handle : _ -> Async<Propulsion.Streams.SpanResult * _>,
                                         maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.StreamsProjector.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval,
                                                  ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSink(log : Serilog.ILogger, stats : Stats, manager : FinalizationProcess.Manager, processingTimeout,
                            maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        let handle = handle processingTimeout manager.Pump
        Config.StartProjector(log, stats, handle, maxReadAhead, maxConcurrentStreams,
                              ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
        
    static member StartSource(log, sink, sourceConfig) =
        Source.start (log, Config.log) sink isReactionStream sourceConfig
