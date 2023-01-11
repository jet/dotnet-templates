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
        match logExternalStats with None -> () | Some f -> f Serilog.Log.Logger

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
    | GroupCheckoutProcess.Category -> true
    | _ -> false

let private handleAction (guestStays : GuestStayAccount.Service) groupCheckoutId (act : GroupCheckoutProcess.Flow.Action) = async {
    match act with
    | GroupCheckoutProcess.Flow.Checkout pendingStays ->
        let attempt stayId groupCheckoutId = async {
            match! guestStays.GroupCheckout(stayId, groupCheckoutId) with
            | GuestStayAccount.Decide.GroupCheckoutResult.Ok r -> return Choice1Of2 (stayId, r) 
            | GuestStayAccount.Decide.GroupCheckoutResult.AlreadyCheckedOut -> return Choice2Of2 stayId } 
        let! outcomes = pendIngStays |> Seq.map (attempt groupCheckoutId) |> Async.parallelThrottled 5
        let residuals, fails = outcomes |> Choice.partition id
        return [ 
            match residuals with
            | [||] -> ()
            | xs -> GroupCheckoutProcess.Events.GuestsMerged {| residuals = [| for stayId, amount in xs -> { stay = stayId; residual = amount } |] |}
            match fails with
            | [||] -> ()
            | stayIds -> GroupCheckoutProcess.Events.GuestsFailed {| stays = stayIds |} ]
    
    | GroupCheckoutProcess.Flow.Ready _
        // Nothing we can do other than wait for the Confirm to Come
    | GroupCheckoutProcess.Flow.Finished ->
        // No processing of any kind can happen after we reach this phase
        return [] }

let private handle handleAction (flow : GroupCheckoutProcess.Service) stream = async {
    match stream with
    | GroupCheckoutProcess.StreamName groupCheckoutId ->
        let! ver' = flow.Pump(groupCheckoutId, handleAction groupCheckoutId)
        return struct (Propulsion.Streams.SpanResult.OverrideWritePosition ver', Outcome.Deferred)
    | other ->
        return failwithf "Span from unexpected category %A" other }

let create store =
    let handleAction =
        let gs = GuestStayAccount.Config.create store
        handleAction gs
    let gcp = GroupCheckoutProcess.Config.create store
    handle handleAction gcp
            
type Config private () =
    
    static member StartSink(log : Serilog.ILogger, stats : Stats, handle,
                            maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams,
                                                (fun stream _events ct -> Async.startImmediateAsTask ct (handle stream)),
                                                stats, stats.StatsInterval.Period,
                                                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Config.log) sink isReactionStream sourceConfig
        
