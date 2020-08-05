﻿[<AutoOpen>]
module private ConsumerTemplate.Infrastructure

open System
open System.Threading.Tasks

type FSharp.Control.Async with
    static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
        Async.FromContinuations <| fun (k, ek, _) ->
            task.ContinueWith (fun (t:Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                    else ek e
                elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                elif t.IsCompleted then k t.Result
                else ek(Exception "invalid Task state!"))
            |> ignore
        static member AwaitTaskCorrect (task : Task) : Async<unit> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k ()
                    else ek(Exception "invalid Task state!"))
                |> ignore

type System.Threading.SemaphoreSlim with

    /// Wait for capacity to be available. Returns false if timeout elapsed before this as achieved
    member semaphore.Await(timeout : TimeSpan) : Async<bool> = async {
        let! ct = Async.CancellationToken
        return! semaphore.WaitAsync(timeout, ct) |> Async.AwaitTaskCorrect
    }

    /// Wait indefinitely for capacity to be available on the semaphore
    member semaphore.Await() : Async<unit> = async {
        let! ct = Async.CancellationToken
        return! semaphore.WaitAsync(ct) |> Async.AwaitTaskCorrect
    }

    /// Throttling wrapper that waits asynchronously until the semaphore has available capacity
    member semaphore.Throttle(workflow : Async<'T>) : Async<'T> = async {
        do! semaphore.Await()
        try return! workflow
        finally semaphore.Release() |> ignore
    }