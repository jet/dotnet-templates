namespace ProjectorTemplate.Consumer.Codec

open Serilog
open System

[<AutoOpen>]
module Codec =
    let parse(x : Propulsion.Streams.IEvent<'T>) =
        { new Equinox.Codec.IEvent<_> with
            member __.EventType = x.EventType
            member __.Data = x.Data
            member __.Meta = x.Meta
            member __.Timestamp = x.Timestamp }
    let tryDecode (codec : Equinox.Codec.IUnionEncoder<_,_>) (log : ILogger) (stream : string) (x : Equinox.Codec.IEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream)
            None
        | x -> x

    type Propulsion.Streams.StreamSpan<'T> with
        // Enumerate the events we've been presented from this stream
        member __.Events : (Equinox.Codec.IEvent<_>) [] =
            __.events |> Array.map parse

    // Enumerate the events in this message
    type Propulsion.Codec.NewtonsoftJson.RenderedSpan with
        member __.Events =
            __.e |> Array.map parse

[<AutoOpen>]
module StreamNameParser = 
    let private catSeparators = [|'-';'_'|]
    let private split (streamName : string) = streamName.Split(catSeparators, 2, StringSplitOptions.RemoveEmptyEntries)
    let category (streamName : string) = let fragments = split streamName in fragments.[0]
    let (|Category|Unknown|) (streamName : string) =
        match split streamName with
        | [| category; id |] -> Category (category, id)
        | _ -> Unknown streamName