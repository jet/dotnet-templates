﻿namespace ConsumerTemplate

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

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// SkuId strongly typed id; represented internally as a string
type SkuId = string<skuId>
and [<Measure>] skuId
module SkuId =
    let toString (value : SkuId) : string = % value
    let parse (value : string) : SkuId = let raw = value in % raw