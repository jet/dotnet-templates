module Shipping.Watchdog.Integration.WatchdogIntegrationTests

open FsCheck
open FsCheck.Xunit
open System

let run (log: Serilog.ILogger) (processManager : Shipping.Domain.FinalizationProcess.Manager) runTimeout (NonEmptyArray batches) = async {

    let counts = System.Collections.Generic.Stack()
    let mutable timeouts = 0
    for GuidStringN tid, GuidStringN cid, IdsAtLeastOne shipmentIds in batches do
        counts.Push shipmentIds.Length
        try let! _ = processManager.TryFinalizeContainer(tid, cid, shipmentIds)
                     |> Async.timeoutAfter runTimeout
            ()
        with :? TimeoutException -> timeouts <- timeouts + 1

    log.Information("Awaiting ({timeouts}/{total} timeouts) batches: {counts}", timeouts, counts.Count, counts) }

[<AbstractClass>]
type ReactorPropertiesBase(reactor : FixtureBase, testOutput) =
    let logSub = reactor.CaptureSerilogLog testOutput
    interface IDisposable with member _.Dispose() = reactor.DumpStats(); logSub.Dispose()
    abstract member RunTimeout : TimeSpan with get
    default _.RunTimeout = TimeSpan.FromSeconds 1.

type MemoryProperties (reactor : MemoryReactor.Fixture, testOutput) =
    inherit ReactorPropertiesBase(reactor, testOutput)

    [<Property(EndSize = 1000, MaxTest = 100)>]
    let run args : Async<unit> = 
        run reactor.Log reactor.ProcessManager reactor.RunTimeout args
    
    // Verify all Committed events submitted to the projector have been processed cleanly, and nothing remains stuck
    // Because we're using a MemoryStore, we can do this deterministically, without arbitrary sleeps to ensure we've seen all events
    // NOTE we do this wait exactly once after we've run all the tests - doing it every time would entail waiting 1 minute per the Timeout
    interface IAsyncDisposable with member _.DisposeAsync() = reactor.AwaitReactions() |> Async.StartAsTask |> System.Threading.Tasks.ValueTask

    interface Xunit.IClassFixture<MemoryReactor.Fixture> // Don't throw away the store or restart projectors per run (academic as we only have one dest for now)

[<Xunit.Collection(CosmosReactor.CollectionName)>]
type CosmosProperties(reactor : CosmosReactor.Fixture, testOutput) =
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 1)>]
#endif    
    let run args = async {
        do! run reactor.Log reactor.ProcessManager reactor.RunTimeout args
        // TODO retrying loop verifying that each started transaction reaches a terminal state
        // For now, the poor-man's version is to look for non-zero Failed and Succeeded counts in the log output after waiting
        do! Async.Sleep 5000
        reactor.DumpStats() }

[<Xunit.Collection(DynamoReactor.CollectionName)>]
type DynamoProperties(reactor : DynamoReactor.Fixture, testOutput) =
    inherit ReactorPropertiesBase(reactor, testOutput)

#if skipIntegrationTests
    // TODO remove the Skip= so you can run the tests
    [<Property(MaxTest = 1, Skip="Cannot run in Equinox.Templates CI environment")>]
#else
    [<Property(MaxTest = 1)>]
#endif    
    let run args = async {
        do! run reactor.Log reactor.ProcessManager reactor.RunTimeout args
        // TODO retrying loop verifying that each started transaction reaches a terminal state
        // For now, the poor-man's version is to look for non-zero Failed and Succeeded counts in the log output after waiting
        do! Async.Sleep 5000
        reactor.DumpStats() }
