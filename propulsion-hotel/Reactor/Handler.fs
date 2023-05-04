module Reactor.Handler

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

let private reactionCategories = [| GroupCheckout.Category |]

// Invocation of the handler is prompted by event notifications from the event store's feed.
// Wherever possible, multiple events get processed together (e.g. in catchup scenarios or where the async checkpointing
//   had not yet managed to mark a new high water mark on the feed before the Reactor got shut down etc)
// Where a handler fails with an exception, the next invocation will also include any further events for that stream that
//   may have arrived since the last handler invocation took place
// NOTE while Propulsion supplies the handler with the full set of outstanding events since the last successful checkpoint,
//   the nature of the reaction processing we are performing here can also be reliant on state that's inferred based on events 
//   prior to those that will have arrived on the feed. For that reason, the caller does not forward the `events` argument here.
let private handle (processor : GroupCheckoutProcess.Service) stream _events = async {
    match stream with
    | GroupCheckout.StreamName groupCheckoutId ->
        let! outcome, ver' = processor.React(groupCheckoutId)
        // For the reasons noted above, processing is carried out based on the reading of the stream for which the handler was triggered.
        // In some cases, due to the clustered nature of the store (and not requiring consistent reads / leader connections
        //   / using session consistency), the set of events read from the stream may not actually include the prompting event. 
        // As a result, we have the Reaction logic inform us of the observed version of the stream, and declare that version
        //   as the position attained on this stream as part of this handler invocation.
        // This, in the case where the read did noy include the prompting event, it'll remain buffered externally, and the handler will
        //   be re-triggered immediately until such time as the observed position on the stream is at or beyond that of the buffered events
        // NOTE also that in some cases, the observed position on the stream can be beyond that which has been notified via
        //   the change feed. In those cases, Propulsion will drop any incoming events that would represent duplication of processing,
        //   (and not even invoke the Handler unless one or more of the feed events are beyond the write position)
        return Propulsion.Sinks.StreamResult.OverrideNextIndex ver', outcome
    | other ->
        return failwithf "Span from unexpected category %A" other }

let private createService store =
    let stays = GuestStay.Factory.create store
    let checkouts = GroupCheckout.Factory.create store
    GroupCheckoutProcess.Service(stays, checkouts, checkoutParallelism = 5)
        
let create store =
    createService store |> handle
            
type Factory private () =
    
    static member StartSink(log, stats, maxConcurrentStreams, handle,maxReadAhead,
                            ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats,
                                                 ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSource(log, sink, sourceConfig) =
        Infrastructure.SourceConfig.start (log, Store.log) sink reactionCategories sourceConfig
