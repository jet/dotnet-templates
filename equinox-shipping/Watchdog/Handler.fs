module Shipping.Watchdog.Handler

open Shipping.Infrastructure
open System

[<RequireQualifiedAccess>]
type Outcome = Completed | Deferred | Resolved of successfully : bool

/// Gathers stats based on the outcome of each Span processed, periodically including them in the Sink summaries
type Stats(log, statsInterval, stateInterval, verboseStore, ?logExternalStats) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable completed, deferred, failed, succeeded = 0, 0, 0, 0

    override _.HandleOk res = res |> function
        | Outcome.Completed -> completed <- completed + 1
        | Outcome.Deferred -> deferred <- deferred + 1
        | Outcome.Resolved successfully -> if successfully then succeeded <- succeeded + 1 else failed <- failed + 1
    override _.DumpStats() =
        base.DumpStats()
        if completed <> 0 || deferred <> 0 || failed <> 0 || succeeded <> 0 then
            log.Information(" Completed {completed} Deferred {deferred} Failed {failed} Succeeded {succeeded}", completed, deferred, failed, succeeded)
            completed <- 0; deferred <- 0; failed <- 0; succeeded <- 0
        match logExternalStats with None -> () | Some f -> let logWithoutContext = Serilog.Log.Logger in f logWithoutContext

    override _.Classify(exn) =
        match exn with
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded -> Propulsion.Streams.OutcomeKind.RateLimited
        | :? Microsoft.Azure.Cosmos.CosmosException as e
            when (e.StatusCode = System.Net.HttpStatusCode.TooManyRequests
                  || e.StatusCode = System.Net.HttpStatusCode.ServiceUnavailable)
                 && not verboseStore -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

open Shipping.Domain

let private reactionCategories = [| FinalizationTransaction.Category |]

let handle
        (processingTimeout : TimeSpan)
        (driveTransaction : Shipping.Domain.TransactionId -> Async<bool>)
        stream span ct = Propulsion.Internal.Async.startImmediateAsTask ct <| async {
    let processingStuckCutoff = let now = DateTimeOffset.UtcNow in now.Add(-processingTimeout)
    match stream, span with
    | TransactionWatchdog.Finalization.MatchStatus (transId, state) ->
        match TransactionWatchdog.toStatus processingStuckCutoff state with
        | TransactionWatchdog.Complete ->
            return struct (Propulsion.Streams.SpanResult.AllProcessed, Outcome.Completed)
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
    
    static member private StartSink(log : Serilog.ILogger, stats : Stats,
                            handle : System.Func<FsCodec.StreamName, Propulsion.Streams.Default.StreamSpan, _,
                                     System.Threading.Tasks.Task<struct (Propulsion.Streams.SpanResult * Outcome)>>,
                            maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval.Period,
                                                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSink(log, stats, manager : FinalizationProcess.Manager, processingTimeout,
                            maxReadAhead, maxConcurrentStreams, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        let handle = handle processingTimeout manager.Pump
        Config.StartSink(log, stats, handle, maxReadAhead, maxConcurrentStreams,
                         ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
        
    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Config.log) sink reactionCategories sourceConfig
        
    static member CreateDynamoSource(log, sink, sourceArgs, trancheIds) =
        SourceConfig.Dynamo.create (log, Config.log) sink reactionCategories sourceArgs (Some trancheIds)
