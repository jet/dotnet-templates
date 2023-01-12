module Reactor.Handler

open Infrastructure
open Propulsion.Internal

[<RequireQualifiedAccess>]
type Outcome = Merged of ok : int * failed : int | Noop

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

let private handleReaction (guestStays : GuestStay.Service) groupCheckoutId (act : GroupCheckout.Flow.Action) = async {
    match act with
    | GroupCheckout.Flow.MergeStays pendingStays ->
        let attempt stayId = async {
            match! guestStays.GroupCheckout(stayId, groupCheckoutId) with
            | GuestStay.Decide.GroupCheckoutResult.Ok r -> return Choice1Of2 (stayId, r) 
            | GuestStay.Decide.GroupCheckoutResult.AlreadyCheckedOut -> return Choice2Of2 stayId } 
        let! outcomes = pendingStays |> Seq.map attempt |> Async.parallelThrottled 5
        let residuals, fails = outcomes |> Choice.partition id
        let outcome = Outcome.Merged (residuals.Length, fails.Length)
        return outcome, [ 
            match residuals with
            | [||] -> ()
            | xs -> GroupCheckout.Events.StaysMerged {| residuals = [| for stayId, amount in xs -> { stay = stayId; residual = amount } |] |}
            match fails with
            | [||] -> ()
            | stayIds -> GroupCheckout.Events.MergesFailed {| stays = stayIds |} ]
    
    | GroupCheckout.Flow.Ready _
        // Nothing we can do other than wait for the Confirm to Come
    | GroupCheckout.Flow.Finished ->
        // No processing of any kind can happen after we reach this phase
        return Outcome.Noop, [] }

let private handle handleReaction (flow : GroupCheckout.Service) stream = async {
    match stream with
    | GroupCheckout.StreamName groupCheckoutId ->
        let! outcome, ver' = flow.React(groupCheckoutId, handleReaction groupCheckoutId)
        return struct (Propulsion.Streams.SpanResult.OverrideWritePosition ver', outcome)
    | other ->
        return failwithf "Span from unexpected category %A" other }

let create store =
    let handleReaction =
        let stays = GuestStay.Config.create store
        handleReaction stays
    let checkout = GroupCheckout.Config.create store
    handle handleReaction checkout
            
type Config private () =
    
    static member StartSink(log : Serilog.ILogger, stats : Stats, handle,
                            maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams,
                                                (fun stream _events ct -> Async.startImmediateAsTask ct (handle stream)),
                                                stats, stats.StatsInterval.Period,
                                                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Config.log) sink isReactionStream sourceConfig
