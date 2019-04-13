[<AutoOpen>]
module SyncTemplate.Infrastructure

open Equinox.Store // AwaitTaskCorrect
open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic

#nowarn "21" // re AwaitKeyboardInterrupt
#nowarn "40" // re AwaitKeyboardInterrupt

type Async with
    static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)
    /// Asynchronously awaits the next keyboard interrupt event
    static member AwaitKeyboardInterrupt () : Async<unit> = 
        Async.FromContinuations(fun (sc,_,_) ->
            let isDisposed = ref 0
            let rec callback _ = Task.Run(fun () -> if Interlocked.Increment isDisposed = 1 then d.Dispose() ; sc ()) |> ignore
            and d : IDisposable = Console.CancelKeyPress.Subscribe callback
            in ())

module Queue =
    let tryDequeue (x : System.Collections.Generic.Queue<'T>) =
#if NET461
        if x.Count = 0 then None
        else x.Dequeue() |> Some
#else
        match x.TryDequeue() with
        | false, _ -> None
        | true, res -> Some res
#endif

type SemaphoreSlim with
    /// F# friendly semaphore await function
    member semaphore.Await(?timeout : TimeSpan) = async {
        let! ct = Async.CancellationToken
        let timeout = defaultArg timeout Timeout.InfiniteTimeSpan
        let task = semaphore.WaitAsync(timeout, ct) 
        return! Async.AwaitTaskCorrect task
    }

    /// Throttling wrapper which waits asynchronously until the semaphore has available capacity
    member semaphore.Throttle(workflow : Async<'T>) : Async<'T> = async {
        let! _ = semaphore.Await()
        try return! workflow
        finally semaphore.Release() |> ignore
    }

type RefCounted<'T> = { mutable refCount: int; value: 'T }

// via https://stackoverflow.com/a/31194647/11635
type SemaphorePool(gen : unit -> SemaphoreSlim) =
    let inners: Dictionary<string, RefCounted<SemaphoreSlim>> = Dictionary()

    let getOrCreateSlot key =
        lock inners <| fun () ->
            match inners.TryGetValue key with
            | true, inner ->
                inner.refCount <- inner.refCount + 1
                inner.value
            | false, _ ->
                let value = gen ()
                inners.[key] <- { refCount = 1; value = value }
                value
    let slotReleaseGuard key : IDisposable =
        { new System.IDisposable with
            member __.Dispose() =
                lock inners <| fun () ->
                    let item = inners.[key]
                    match item.refCount with
                    | 1 -> inners.Remove key |> ignore
                    | current -> item.refCount <- current - 1 }

    member __.ExecuteAsync(k,f) = async {
        let x = getOrCreateSlot k
        use _ = slotReleaseGuard k
        return! f x }

    member __.Execute(k,f) =
        let x = getOrCreateSlot k
        use _l = slotReleaseGuard k
        f x