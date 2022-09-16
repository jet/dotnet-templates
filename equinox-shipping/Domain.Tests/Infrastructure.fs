[<AutoOpen>]
module Shipping.Domain.Tests.Infrastructure

open System.Collections.Concurrent

type EventAccumulator<'E>() =
    let messages = ConcurrentDictionary<struct (string * string), ConcurrentQueue<'E>>()

    member _.Record(struct (categoryName, streamId, events : 'E array)) =
        let initStreamQueue _ = ConcurrentQueue events
        let appendToQueue _ (queue : ConcurrentQueue<'E>) = events |> Seq.iter queue.Enqueue; queue
        messages.AddOrUpdate(struct (categoryName, streamId), initStreamQueue, appendToQueue) |> ignore

    member _.Queue(stream) =
        match messages.TryGetValue stream with
        | false, _ -> Seq.empty<'E>
        | true, xs -> xs :> _

    member _.All() = seq { for KeyValue (_, xs) in messages do yield! xs }

    member _.Clear() =
        messages.Clear()
