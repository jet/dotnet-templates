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
    let private tryDecode (codec : Equinox.Codec.IUnionEncoder<_,_>) (log : ILogger) (stream : string) (x : Equinox.Codec.IEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream)
            None
        | x -> x

    let tryPickNewest log (codec : Equinox.Codec.IUnionEncoder<'summary,_>) (stream, span : Propulsion.Streams.StreamSpan<_>) : (int64*'summary) option =
        span.events
        |> Seq.mapi (fun i x -> span.index + int64 i, toCodecEvent x)
        |> Seq.rev
        |> Seq.tryPick (fun (v,e) -> match tryDecode codec log stream e with Some d -> Some (v,d) | None -> None)

[<AutoOpen>]
module CodecConventions =
    /// Allows one to hook in any JsonConverters etc
    let serializationSettings = Newtonsoft.Json.JsonSerializerSettings()
    /// Automatically generates a Union Codec based using the scheme described in https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/
    let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.Codec.NewtonsoftJson.Json.Create<'Union>(serializationSettings)

[<AutoOpen>]
module StreamNameParser =
    let private catSeparators = [|'-'|]
    let private split (streamName : string) = streamName.Split(catSeparators, 2, StringSplitOptions.RemoveEmptyEntries)
    let category (streamName : string) = let fragments = split streamName in fragments.[0]
    let (|Category|Unknown|) (streamName : string) =
        match split streamName with
        | [| category; id |] -> Category (category, id)
        | _ -> Unknown streamName