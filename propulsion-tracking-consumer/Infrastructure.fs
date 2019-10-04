namespace ConsumerTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module StreamCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IUnionEncoder<_,_>) (log : Serilog.ILogger) (stream : string) (x : FsCodec.IIndexedEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream)
            None
        | x -> x

// TODO use one included in Propulsion.Kafka.Core
/// StreamsConsumer buffers and deduplicates messages from a contiguous stream with each message bearing an index.
/// The messages we consume don't have such characteristics, so we generate a fake `index` by keeping an int per stream in a dictionary
type StreamKeyEventSequencer() =
    // we synthesize a monotonically increasing index to render the deduplication facility inert
    let indices = System.Collections.Generic.Dictionary()
    let genIndex streamName =
        match indices.TryGetValue streamName with
        | true, v -> let x = v + 1 in indices.[streamName] <- x; int64 x
        | false, _ -> let x = 0 in indices.[streamName] <- x; int64 x

    // Stuff the full content of the message into an Event record - we'll parse it when it comes out the other end in a span
    member __.ToStreamEvent(KeyValue (k,v : string), ?eventType) : Propulsion.Streams.StreamEvent<byte[]> seq =
        let eventType = defaultArg eventType String.Empty
        let e = FsCodec.Core.IndexedEventData(genIndex k,false,eventType,System.Text.Encoding.UTF8.GetBytes v,null,DateTimeOffset.UtcNow)
        Seq.singleton { stream=k; event=e }

/// SkuId strongly typed id; represented internally as a string
type SkuId = string<skuId>
and [<Measure>] skuId
module SkuId =
    let toString (value : SkuId) : string = % value
    let parse (value : string) : SkuId = let raw = value in % raw