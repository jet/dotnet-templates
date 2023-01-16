module Reactor.GroupCheckoutProcess

open Domain
open Infrastructure

[<RequireQualifiedAccess>]
type Outcome = Merged of ok : int * failed : int | Noop

type Service(guestStays : GuestStay.Service, groupCheckouts : GroupCheckout.Service, checkoutParallelism) =
    
    // Attempts a single merge for the specified stay
    let attemptMerge groupCheckoutId stayId = async {
        match! guestStays.GroupCheckout(stayId, groupCheckoutId) with
        | GuestStay.Decide.GroupCheckoutResult.Ok r -> return Choice1Of2 (stayId, r) 
        | GuestStay.Decide.GroupCheckoutResult.AlreadyCheckedOut -> return Choice2Of2 stayId }
    
    // Runs all outstanding merges (with limited parallelism)
    // Yields residual charges for ones that succeeded,
    // along with the ids of any that have already been checked out and hence cannot proceed as part of the group checkout
    let executeMergeStayAttempts groupCheckoutId stayIds = async {
        let! outcomes = stayIds |> Seq.map (attemptMerge groupCheckoutId) |> Async.parallelLimit checkoutParallelism
        return outcomes |> Choice.partition id }
                
    // Attempts to merge the specified stays into the specified Group Checkout
    // Maps the results of each individual merge attempt into 0, 1 or 2 events reflecting the progress achieved against the requested merges 
    let decideMerge groupCheckoutId stayIds : Async<Outcome * GroupCheckout.Events.Event list> = async {
        let! residuals, fails = executeMergeStayAttempts groupCheckoutId stayIds
        let events = [ 
            match residuals with
            | [||] -> ()
            | xs -> GroupCheckout.Events.StaysMerged {| residuals = [| for stayId, amount in xs -> { stay = stayId; residual = amount } |] |}
            match fails with
            | [||] -> ()
            | stayIds -> GroupCheckout.Events.MergesFailed {| stays = stayIds |} ]
        let outcome = Outcome.Merged (residuals.Length, fails.Length)
        return outcome, events }
    
    let handleReaction groupCheckoutId act = async {
        match act with
        | GroupCheckout.Flow.MergeStays pendingStays ->
            return! decideMerge groupCheckoutId pendingStays
        | GroupCheckout.Flow.Ready _
            // Nothing we can do other than wait for the Confirm to Come
        | GroupCheckout.Flow.Finished ->
            // No processing of any kind can happen after we reach this phase
            return Outcome.Noop, [] }
            
    /// Handles Reactions based on the state of the Group Checkout's workflow
    /// NOTE result includes the post-version of the stream after processing has concluded
    member _.React(groupCheckoutId) : Async<Outcome * int64> = 
        groupCheckouts.React(groupCheckoutId, handleReaction groupCheckoutId)
