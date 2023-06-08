[<AutoOpen>]
module Shipping.Domain.Tests.Infrastructure

open System.Collections.Concurrent

type EventAccumulator<'E>() =
    let messages = ConcurrentDictionary<FsCodec.StreamName, ConcurrentQueue<'E>>()

    member _.Record(struct (streamName, events: 'E[])) =
        let initStreamQueue _ = ConcurrentQueue events
        let appendToQueue _ (queue: ConcurrentQueue<'E>) = events |> Seq.iter queue.Enqueue; queue
        messages.AddOrUpdate(streamName, initStreamQueue, appendToQueue) |> ignore

    member _.Queue(stream) =
        match messages.TryGetValue stream with
        | false, _ -> Seq.empty<'E>
        | true, xs -> xs :> _
    member x.Queue(cat, sid) = x.Queue(FsCodec.StreamName.create cat (Equinox.Core.StreamId.toString sid))
    
    member _.All() = seq { for KeyValue (_, xs) in messages do yield! xs }

    member _.Clear() = messages.Clear()
