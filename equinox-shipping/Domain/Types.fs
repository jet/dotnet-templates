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

namespace global

module Equinox =

    /// Tag log entries so we can filter them out if logging to the console
    let log = Serilog.Log.ForContext("isMetric", true)
    let createDecider stream = Equinox.Decider(log, stream, maxAttempts = 3)
