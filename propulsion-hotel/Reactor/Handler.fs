module Reactor.Handler

open Infrastructure
open Propulsion.Internal

[<RequireQualifiedAccess>]
type Outcome = Completed | Deferred | Resolved of successfully : bool

/// Gathers stats based on the outcome of each Span processed, periodically including them in the Sink summaries
type Stats(log, statsInterval, stateInterval, ?logExternalStats) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable completed, deferred, failed, succeeded = 0, 0, 0, 0

    override _.DumpStats() =
        base.DumpStats()
        if completed <> 0 || deferred <> 0 || failed <> 0 || succeeded <> 0 then
            log.Information(" Completed {completed} Deferred {deferred} Failed {failed} Succeeded {succeeded}", completed, deferred, failed, succeeded)
            completed <- 0; deferred <- 0; failed <- 0; succeeded <- 0
        match logExternalStats with None -> () | Some f -> let logWithoutContext = Serilog.Log.Logger in f logWithoutContext

    override _.HandleOk res = res |> function
        | Outcome.Completed -> completed <- completed + 1
        | Outcome.Deferred -> deferred <- deferred + 1
        | Outcome.Resolved successfully -> if successfully then succeeded <- succeeded + 1 else failed <- failed + 1
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

let private handleAction (guestStays : GuestStay.Service) groupCheckoutId (act : GroupCheckout.Flow.Action) = async {
    match act with
    | GroupCheckout.Flow.Checkout pendingStays ->
        let attempt stayId groupCheckoutId = async {
            match! guestStays.GroupCheckout(stayId, groupCheckoutId) with
            | GuestStay.Decide.GroupCheckoutResult.Ok r -> return Choice1Of2 (stayId, r) 
            | GuestStay.Decide.GroupCheckoutResult.AlreadyCheckedOut -> return Choice2Of2 stayId } 
        let! outcomes = pendingStays |> Seq.map (attempt groupCheckoutId) |> Async.parallelThrottled 5
        let residuals, fails = outcomes |> Choice.partition id
        return [ 
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
        return [] }

let private handle handleAction (flow : GroupCheckout.Service) stream = async {
    match stream with
    | GroupCheckout.StreamName groupCheckoutId ->
        let! ver' = flow.React(groupCheckoutId, handleAction groupCheckoutId)
        return struct (Propulsion.Streams.SpanResult.OverrideWritePosition ver', Outcome.Deferred)
    | other ->
        return failwithf "Span from unexpected category %A" other }

let create store =
    let handleReaction =
        let stays = GuestStay.Config.create store
        handleAction stays
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
        
