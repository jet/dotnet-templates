[<AutoOpen>]
module Shipping.Watchdog.Integration.Infrastructure

open System

module Async =

    /// Wraps a computation, cancelling (and triggering a timeout exception) if it doesn't complete within the specified timeout
    let timeoutAfter (timeout : TimeSpan) (c : Async<'a>) = async {
        let! r = Async.StartChild(c, int timeout.TotalMilliseconds)
        return! r }
