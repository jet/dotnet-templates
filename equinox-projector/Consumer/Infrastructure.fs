[<AutoOpen>]
module private ProjectorTemplate.Consumer.Infrastructure

open Equinox.Projection // semaphore.Await

type System.Threading.SemaphoreSlim with
    /// Throttling wrapper which waits asynchronously until the semaphore has available capacity
    member semaphore.Throttle(workflow : Async<'T>) : Async<'T> = async {
        let! _ = semaphore.Await()
        try return! workflow
        finally semaphore.Release() |> ignore
    }