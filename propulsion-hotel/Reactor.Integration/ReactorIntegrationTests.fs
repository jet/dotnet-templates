module Reactor.Integration.ReactorIntegrationTests

open Domain
open FsCheck
open FsCheck.Xunit
open System

let runCheckoutScenario store (paymentId, id, NonEmptyArray stays, payBefore) checkWithRetry = async {
    let staysService = GuestStay.Factory.create store
    let checkoutService = GroupCheckout.Factory.create store            
    let mutable charged = 0
    for stayId, chargeId, PositiveInt amount in stays do
        charged <- charged + amount
        do! staysService.Charge(stayId, chargeId, amount)
    let stays = [| for stayId, _, _ in stays -> stayId |]
    if payBefore then do! checkoutService.Pay(id, paymentId, charged)
    let! _ = checkoutService.Merge(id, stays)
    // Each part of a scenario that is reliant on the triggering and completion of a reaction is wrapped in this manner
    // to cover cases where the reaction work has not been processed by the time the `wait ()` has timed out
    do! checkWithRetry "awaiting Group Checkout action" <| fun wait -> async {
        do! wait ()
        match! checkoutService.Confirm(id) with
        | GroupCheckout.Decide.Ok -> ()
        | GroupCheckout.Decide.Processing ->
            failwith "still busy" // Trigger the retry to wait for processing to complete
            // NOTE when testing with the MemoryStoreSource, the `wait` should be deterministic in nature, i.e.
            //      after the `wait` call, we know that all reactions have been processed, so assertions like this,
            //      that are intended to confirm that required effects have been achieved should always succeed.
            //      For concrete stores, or or more retries may be necessary 
        | GroupCheckout.Decide.BalanceOutstanding _ ->
            if payBefore then
                // This state should not arise, unless there's a logic bug somewhere; throw to make the test fail 
                failwith "Should have been paid" 
    }
    if payBefore then
        return true
    else
        do! checkoutService.Pay(id, paymentId, charged)
        match! checkoutService.Confirm(id) with
        | GroupCheckout.Decide.Ok -> return true
        | GroupCheckout.Decide.Processing
        | GroupCheckout.Decide.BalanceOutstanding _ -> return false }

[<AbstractClass>]
type ReactorPropertiesBase(reactor : FixtureBase, testOutput) =
    let logSub = reactor.CaptureSerilogLog testOutput
    
    abstract member DisposeAsync : unit -> Async<unit>
    default _.DisposeAsync() = async.Zero ()

    // Abusing IDisposable rather than IAsyncDisposable as we want the output to accompany the test output
    interface IDisposable with
        member x.Dispose() = Async.RunSynchronously <| async {
            do! x.DisposeAsync()
            // Trigger the logging proactively here, before we lose the ability to log
            reactor.DumpStats()
            logSub.Dispose() }

type MemoryProperties (reactor : MemoryReactor.Fixture, testOutput) =
    // Trigger logging of (Aggregate) Reactor stats after each Test/Property is run
    inherit ReactorPropertiesBase(reactor, testOutput)

    [<Property(EndSize = 1000, MaxTest = 10)>]
    let run args : Async<bool> =
        runCheckoutScenario reactor.Store args reactor.CheckReactions
   
    override _.DisposeAsync() =
        // Validate nothing is left hanging; This is deterministic and quick with a MemoryStoreSource
        reactor.Wait()
     
    // Use a single Store (and Reactor) across Tests (academic as we only have one Test for now)
    interface Xunit.IClassFixture<MemoryReactor.Fixture>

[<Xunit.Collection(DynamoReactor.CollectionName)>]
type DynamoProperties(reactor : DynamoReactor.Fixture, testOutput) =
    // Failsafe to emit the Remaining stats even in the case of a Test/Property failing (in success case, it's redundant)
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 2)>]
#endif    
    let run args : Async<bool> = async {
        try return! runCheckoutScenario reactor.Store args reactor.CheckReactions
        // Dump the stats after each and every iteration of the test
        finally reactor.DumpStats() }
    
    // Verify all Committed events submitted to the projector have been processed cleanly, and nothing remains stuck
    // Because we're using a DynamoStore, this is not entirely deterministic, but the fact the MemoryStore test exits cleanly every time gives us adequate confidence
    // NOTE we do this wait exactly once after we've run all the tests - doing it every time would entail waiting for a long period after each run of the property
    override _.DisposeAsync() =
        reactor.Wait()

[<Xunit.Collection(MessageDbReactor.CollectionName)>]
type MessageDbProperties(reactor : MessageDbReactor.Fixture, testOutput) =
    // Failsafe to emit the Remaining stats even in the case of a Test/Property failing (in success case, it's redundant)
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 2)>]
#endif    
    let run args : Async<bool> = async {
        try return! runCheckoutScenario reactor.Store args reactor.CheckReactions
        // Dump the stats after each and every iteration of the test
        finally reactor.DumpStats() }
    
    // Verify all Committed events submitted to the projector have been processed cleanly, and nothing remains stuck
    // The fact the MemoryStore test exits cleanly every time gives us confidence that no straggler events remain after ANY case
    // NOTE we do this wait exactly once after we've run all the tests - doing it every time would entail waiting for a long period after each run of the property
    override _.DisposeAsync() =
        reactor.Wait()
