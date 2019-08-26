namespace ConsumerTemplate

open Serilog
open System

module StreamCodec =

    /// Adapts a pending event record to the canonical event record interface specified by `Equinox.Codec`
    let private toCodecEvent (x : Propulsion.Streams.IEvent<'T>) =
        { new Equinox.Codec.IEvent<_> with
            member __.EventType = x.EventType
            member __.Data = x.Data
            member __.Meta = x.Meta
            member __.Timestamp = x.Timestamp }

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : Equinox.Codec.IUnionEncoder<_,_>) (log : ILogger) (stream : string) (x : Propulsion.Streams.IEvent<byte[]>) =
        match toCodecEvent x |> codec.TryDecode with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream)
            None
        | x -> x

[<AutoOpen>]
module StreamNameParser = 
    let private catSeparators = [|'-'|]
    let private split (streamName : string) = streamName.Split(catSeparators, 2, StringSplitOptions.RemoveEmptyEntries)
    let category (streamName : string) = let fragments = split streamName in fragments.[0]
    let (|Category|Unknown|) (streamName : string) =
        match split streamName with
        | [| category; id |] -> Category (category, id)
        | _ -> Unknown streamName