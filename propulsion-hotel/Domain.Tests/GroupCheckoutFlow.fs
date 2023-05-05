module Domain.Tests.GroupCheckoutFlow

open Domain
open FsCheck
open FsCheck.Xunit
open Reactor
open Swensen.Unquote

[<Property>]
let ``Happy path including Reaction`` (store, groupCheckoutId, paymentId, stays: _ []) = async {
    let staysService = GuestStay.Factory.create store
    let sut = GroupCheckout.Factory.create store
    let processor = GroupCheckoutProcess.Service(staysService, sut, 2)
    let mutable charged = 0
    for stayId, chargeId, PositiveInt amount in stays do
        charged <- charged + amount
        do! staysService.Charge(stayId, chargeId, amount)
    let stays = [| for stayId, _, _ in stays -> stayId |]

    match! sut.Merge(groupCheckoutId, stays) with
    // If no stays were added to the group checkout, no checkouts actions should be pending
    | GroupCheckout.Flow.Ready 0m ->
        // We'll run the Reactor, but only to confirm it is a no-op
        [||] =! stays
        match! processor.React(groupCheckoutId) with
        | GroupCheckoutProcess.Outcome.Noop, _ -> ()
        | GroupCheckoutProcess.Outcome.Merged _, _ -> failwith "Should be noop"
    // If any stays have been added, they should be recorded, and work should be triggered
    | GroupCheckout.Flow.MergeStays staysToDo ->
        staysToDo =! stays
        match! processor.React(groupCheckoutId) with
        | GroupCheckoutProcess.Outcome.Merged (ok, fail), _ -> test <@ ok = stays.Length && 0 = fail @>
        | GroupCheckoutProcess.Outcome.Noop, _ -> failwith "Should not be noop"
    // We should not end up in any other states
    | GroupCheckout.Flow.Ready _
    | GroupCheckout.Flow.Finished -> failwith "unexpected"
    
    do! sut.Pay(groupCheckoutId, paymentId, charged)
    
    let! _ = sut.Confirm(groupCheckoutId)
    
    let! next = sut.Read(groupCheckoutId)
    test <@ GroupCheckout.Flow.Finished = next @>
}
