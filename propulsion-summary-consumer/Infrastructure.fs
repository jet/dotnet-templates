namespace ConsumerTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module EventCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IUnionEncoder<_, _, _>) (log : Serilog.ILogger) (stream : string) (x : FsCodec.ITimelineEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream)
            None
        | x -> x

[<AutoOpen>]
module StreamName =
    let private catSeparators = [|'-'|]
    let private split (sep : char[]) (streamName : string) = streamName.Split(sep, 2, StringSplitOptions.RemoveEmptyEntries)
    let category (streamName : string) = let fragments = split catSeparators streamName in fragments.[0]
    let (|Two|_|) (sep : char[]) value =
        match split sep value with
        | [| category; id |] -> Some (category, id)
        | _ -> None
    let (|Category|Other|) (streamName : string) =
        match streamName with
        | Two catSeparators (category, id) -> Category (category, id)
        | _ -> Other streamName

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value : ClientId) : string = Guid.toStringN %value
    let parse (value : string) : ClientId = let raw = Guid.Parse value in % raw