module Reactor.Handler

open Infrastructure
open Propulsion.Internal

type Outcome = GroupCheckoutProcess.Outcome

/// Gathers stats based on the outcome of each Span processed, periodically including them in the Sink summaries
type Stats(log, statsInterval, stateInterval, ?logExternalStats) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable completed, ignored, failed, succeeded = 0, 0, 0, 0

    override _.DumpStats() =
        base.DumpStats()
        if completed <> 0 || ignored <> 0 || failed <> 0 || succeeded <> 0 then
            log.Information(" Merged {completed} failed {failed} succeeded {succeeded} Ignored {ignored} ", completed, failed, succeeded, ignored)
            completed <- 0; failed <- 0; succeeded <- 0; ignored <- 0
        match logExternalStats with None -> () | Some f -> let logWithoutContext = Serilog.Log.Logger in f logWithoutContext

    override _.HandleOk res = res |> function
        | Outcome.Merged (ok, denied)-> completed <- completed + 1; succeeded <- succeeded + ok; failed <- failed + denied
        | Outcome.Noop -> ignored <- ignored + 1
    override _.Classify(exn) =
        match exn with
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

open Domain

let private isReactionStream = function
    | GroupCheckout.Category -> true
    | _ -> false

let private handle (processor : GroupCheckoutProcess.Service) stream = async {
    match stream with
    | GroupCheckout.StreamName groupCheckoutId ->
        let! outcome, ver' = processor.React(groupCheckoutId)
        // Checkpointing happens asynchronously. When the process is started up we might reprocess
        // already handled messages. The service returns the version of the process manager stream
        // so we can use that to fast-forward our processing and avoid re-triggering actions unnecessarily
        return struct (Propulsion.Streams.SpanResult.OverrideWritePosition ver', outcome)
    | other ->
        return failwithf "Span from unexpected category %A" other }

let create store =
    let processor =
        let stays = GuestStay.Config.create store
        let checkouts = GroupCheckout.Config.create store
        GroupCheckoutProcess.Service(stays, checkouts)
    handle processor
            
type Config private () =
    
    static member StartSink(log : Serilog.ILogger, stats : Stats, handle,
                            maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams,
                                                (fun stream _events ct -> Async.startImmediateAsTask ct (handle stream)),
                                                stats, stats.StatsInterval.Period,
                                                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Config.log) sink isReactionStream sourceConfig
