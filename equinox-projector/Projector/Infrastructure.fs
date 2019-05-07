[<AutoOpen>]
module private ProjectorTemplate.Projector.Infrastructure

open System
open System.Threading
open System.Threading.Tasks

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

// TODO remove when using 2.0.0-preview7
open Equinox.Projection.Codec

module RenderedEvent =
    let ofStreamItem (x: Equinox.Projection.StreamItem) : RenderedEvent =
        { s = x.stream; i = x.index; c = x.event.EventType; t = x.event.Timestamp; d = x.event.Data; m = x.event.Meta }