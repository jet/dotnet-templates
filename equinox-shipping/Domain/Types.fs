namespace Shipping.Domain

open FSharp.UMX

[<Measure>] type shipmentId
type ShipmentId = string<shipmentId>
module ShipmentId =
    let toString (x : ShipmentId) : string = %x

[<Measure>] type containerId
type ContainerId = string<containerId>
module ContainerId =
    let toString (x : ContainerId) : string = %x

[<Measure>] type transactionId
type TransactionId = string<transactionId>
module TransactionId =
    let toString (x : TransactionId) : string = %x
    let parse (x : string) = %x
    let (|Parse|) = parse

module Guid =

    let inline toStringN (x : System.Guid) = x.ToString "N"
    let generateStringN () = let g = System.Guid.NewGuid() in toStringN g
