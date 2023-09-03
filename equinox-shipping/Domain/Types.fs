namespace Shipping.Domain

open FSharp.UMX

[<Measure>] type shipmentId
type ShipmentId = string<shipmentId>
module ShipmentId =
    let toString (x: ShipmentId): string = %x

[<Measure>] type containerId
type ContainerId = string<containerId>
module ContainerId =
    let toString (x: ContainerId): string = %x

[<Measure>] type transactionId
type TransactionId = string<transactionId>
module TransactionId =
    let toString (x: TransactionId): string = %x
    let parse (x: string): TransactionId = %x
    let (|Parse|) = parse

type internal CategoryId<'ids>(name, gen: 'ids -> FsCodec.StreamId) =
    member val Category = name
    member _.Gen = gen
    member _.Name = gen >> FsCodec.StreamName.create name
type internal CategoryIdParseable<'ids>(name, gen: 'ids -> FsCodec.StreamId, dec: FsCodec.StreamId -> 'ids) =
    inherit CategoryId<'ids>(name, gen)
    member _.DecodeId = dec
    member _.TryDecode = FsCodec.StreamName.tryFind name >> ValueOption.map dec

module Guid =

    let inline toStringN (x: System.Guid) = x.ToString "N"
    let generateStringN () = let g = System.Guid.NewGuid() in toStringN g
