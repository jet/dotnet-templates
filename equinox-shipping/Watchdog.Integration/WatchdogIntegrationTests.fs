namespace Shipping.Watchdog.Integration

open FsCheck
open FsCheck.Xunit
open System

type MemoryProperties(testOutput) =

    [<Property(StartSize=1000, MaxTest=5)>]
    let run (NonEmptyArray batches) = async {

        use reactor = new MemoryReactorFixture(testOutput) // Run under debugger and/or adjust XunitLogger.minLevel to see events in test output
        let counts = System.Collections.Generic.Stack()
        let mutable timeouts = 0
        for GuidStringN tid, GuidStringN cid, IdsAtLeastOne shipmentIds in batches do
            counts.Push shipmentIds.Length
            try let! _ = reactor.ProcessManager.TryFinalizeContainer(tid, cid, shipmentIds)
                         |> Async.timeoutAfter reactor.RunTimeout
                ()
            with :? TimeoutException -> timeouts <- timeouts + 1

        reactor.Log.Information("Awaiting batches: {counts} ({timeouts}/{total} timeouts)", counts, timeouts, counts.Count)
        do! reactor.AwaitWithStopOnCancellation() }
