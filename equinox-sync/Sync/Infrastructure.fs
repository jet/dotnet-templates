[<AutoOpen>]
module SyncTemplate.Infrastructure

open Equinox.Store // AwaitTaskCorrect
open System
open System.Threading
open System.Threading.Tasks

//#nowarn "21" // re AwaitKeyboardInterrupt
//#nowarn "40" // re AwaitKeyboardInterrupt

type Async with
    static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)
    /// Asynchronously awaits the next keyboard interrupt event
    static member AwaitKeyboardInterrupt () : Async<unit> = 
        Async.FromContinuations(fun (sc,_,_) ->
            let isDisposed = ref 0
            let rec callback _ = Task.Run(fun () -> if Interlocked.Increment isDisposed = 1 then d.Dispose() ; sc ()) |> ignore
            and d : IDisposable = Console.CancelKeyPress.Subscribe callback
            in ())

//module Queue =
//    let tryDequeue (x : System.Collections.Generic.Queue<'T>) =
//#if NET461
//        if x.Count = 0 then None
//        else x.Dequeue() |> Some
//#else
//        match x.TryDequeue() with
//        | false, _ -> None
//        | true, res -> Some res
//#endif

type SemaphoreSlim with
    /// F# friendly semaphore await function
    member semaphore.Await(?timeout : TimeSpan) = async {
        let! ct = Async.CancellationToken
        let timeout = defaultArg timeout Timeout.InfiniteTimeSpan
        let task = semaphore.WaitAsync(timeout, ct) 
        return! Async.AwaitTaskCorrect task
    }

//    /// Throttling wrapper which waits asynchronously until the semaphore has available capacity
//    member semaphore.Throttle(workflow : Async<'T>) : Async<'T> = async {
//        let! _ = semaphore.Await()
//        try return! workflow
//        finally semaphore.Release() |> ignore
//    }