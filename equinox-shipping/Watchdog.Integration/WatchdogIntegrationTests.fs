module Shipping.Watchdog.Integration.WatchdogIntegrationTests

open FsCheck
open FsCheck.Xunit
open Propulsion.Reactor.Internal // Async.timeoutAfter
open System

let run (log: Serilog.ILogger) (processManager: Shipping.Domain.FinalizationProcess.Manager) runTimeout check (NonEmptyArray batches) = async {

    let counts = System.Collections.Generic.Stack()
    let mutable timeouts = 0
    for GuidStringN tid, GuidStringN cid, NonEmptyArray shipmentIds in batches do
        counts.Push shipmentIds.Length
        try let! _ = processManager.TryFinalizeContainer(tid, cid, shipmentIds)
                     |> Async.timeoutAfter runTimeout
            ()
        with :? TimeoutException -> timeouts <- timeouts + 1

    do! check "await watchdog sign off" <| fun wait -> async {
        log.Information("Processing Completed. Timeouts {timeouts}/{total} Batches {counts}", timeouts, counts.Count, counts)
        do! wait ()
        (* TODO validate that the container got finalized or rolled back and was not left in an inconclusive state *)
    }
}    

[<AbstractClass>]
type ReactorPropertiesBase(reactor: FixtureBase, testOutput) =
    let logSub = reactor.CaptureSerilogLog testOutput
    
    abstract member DisposeAsync: unit -> Async<unit>
    default _.DisposeAsync() = async.Zero ()

    abstract member RunTimeout: TimeSpan with get
    default _.RunTimeout = TimeSpan.FromSeconds 1.

    // Abusing IDisposable rather than IAsyncDisposable as we want the output to accompany the test output
    interface IDisposable with
        member x.Dispose() = Async.RunSynchronously <| async {
            do! x.DisposeAsync()
            // Trigger the logging proactively here, before we lose the ability to log
            reactor.DumpStats()
            logSub.Dispose() }
        
type MemoryProperties (reactor: MemoryReactor.Fixture, testOutput) =
    // Trigger logging of (Aggregate) Reactor stats after each Test/Propery is run
    inherit ReactorPropertiesBase(reactor, testOutput)

    [<Property(EndSize = 1000, MaxTest = 10)>]
    let run args: Async<unit> =
        run reactor.Log reactor.ProcessManager reactor.RunTimeout reactor.CheckReactions args
   
    override _.DisposeAsync() =
        // Validate nothing is left hanging; This is deterministic and quick with a MemoryStoreSource
        reactor.Wait()
     
    // Use a single Store (and Reactor) across Tests (academic as we only have one Test for now)
    interface Xunit.IClassFixture<MemoryReactor.Fixture>

[<Xunit.Collection(CosmosReactor.CollectionName)>]
type CosmosProperties(reactor: CosmosReactor.Fixture, testOutput) =
    // Failsafe to emit the Remaining stats even in the case of a Test/Property failing (in success case, it's redundant)
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 1)>]
#endif    
    let run args: Async<unit> = async {
        do! run reactor.Log reactor.ProcessManager reactor.RunTimeout reactor.CheckReactions args
        // Dump the stats after each and every iteration of the test
        reactor.DumpStats() }

    // Verify all Committed events submitted to the projector have been processed cleanly, and nothing remains stuck
    // Because we're using a CosmosStore, there is no good story for how to do this, but the fact the MemoryStore test exits cleanly every time gives us adequate confidence
    // For now, the poor-man's version would be to look for non-zero Failed and Succeeded counts in the log output after waiting
    // NOTE we do this wait exactly once after we've run all the tests - doing it every time would entail waiting 1 minute per the Timeout
    // override _.DisposeAsync() = async {
    //     do! Async.Sleep(60 * 1000)
    //     (* TODO implement reactor.Wait() *) }
    
[<Xunit.Collection(DynamoReactor.CollectionName)>]
type DynamoProperties(reactor: DynamoReactor.Fixture, testOutput) =
    // Failsafe to emit the Remaining stats even in the case of a Test/Property failing (in success case, it's redundant)
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 2)>]
#endif    
    let run args: Async<unit> = async {
        do! run reactor.Log reactor.ProcessManager reactor.RunTimeout reactor.CheckReactions args
        // Dump the stats after each and every iteration of the test
        reactor.DumpStats() }
    
    // Verify all Committed events submitted to the projector have been processed cleanly, and nothing remains stuck
    // Because we're using a DynamoStore, this is not entirely deterministic, but the fact the MemoryStore test exits cleanly every time gives us adequate confidence
    // NOTE we do this wait exactly once after we've run all the tests - doing it every time would entail waiting for a long period after each run of the property
    override _.DisposeAsync() =
        reactor.Wait()

[<Xunit.Collection(EsdbReactor.CollectionName)>]
type EsdbProperties(reactor: EsdbReactor.Fixture, testOutput) =
    // Failsafe to emit the Remaining stats even in the case of a Test/Property failing (in success case, it's redundant)
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 10)>]
#endif    
    let run args: Async<unit> = async {
        do! run reactor.Log reactor.ProcessManager reactor.RunTimeout reactor.CheckReactions args
        // Dump the stats after each and every iteration of the test
        reactor.DumpStats() }
    
    // Verify all Committed events submitted to the projector have been processed cleanly, and nothing remains stuck
    // Because we're using EventStoreDb, this is not entirely deterministic, but the fact the MemoryStore test exits cleanly every time gives us adequate confidence
    // NOTE we do this wait exactly once after we've run all the tests - doing it every time would entail waiting for a long period after each run of the property
    override _.DisposeAsync() =
        reactor.Wait()
