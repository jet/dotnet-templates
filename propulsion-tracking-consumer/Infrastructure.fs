namespace ConsumerTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings

module Log =

    let (|LogAsWarning|_|) = function (e : exn) -> Serilog.Log.Warning(e, "Unhandled"); None
    let exceptions f = async { try return! f with LogAsWarning -> return! invalidOp "not possible; Filter always returns None" }
    
module EventCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IEventCodec<_, _, _>) (log : Serilog.ILogger) streamName (x : FsCodec.ITimelineEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, streamName)
            None
        | x -> x

/// SkuId strongly typed id; represented internally as a string
type SkuId = string<skuId>
and [<Measure>] skuId
module SkuId =
    let toString (value : SkuId) : string = % value
    let parse (value : string) : SkuId = let raw = value in % raw
    let (|Parse|) = parse