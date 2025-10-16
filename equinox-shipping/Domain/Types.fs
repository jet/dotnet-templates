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

namespace global

type DataMemberAttribute = System.Runtime.Serialization.DataMemberAttribute

module Seq =

    let inline chooseV f xs = seq { for x in xs do match f x with ValueSome v -> yield v | ValueNone -> () }

module Async =

    let parallelLimit maxDop workflows = Async.Parallel(workflows, maxDegreeOfParallelism = maxDop)
    let parallelDoLimit maxDop (workflows: seq<Async<unit>>) = parallelLimit maxDop workflows |> Async.Ignore<unit[]>

module Guid =

    let inline gen () = System.Guid.NewGuid()
    let inline toStringN (x: System.Guid) = x.ToString "N"

/// Handles symmetric generation and decoding of StreamNames composed of a series of elements via the FsCodec.StreamId helpers
type internal CategoryId<'elements>(name, gen: 'elements -> FsCodec.StreamId, dec: FsCodec.StreamId -> 'elements) =
    member _.StreamName = gen >> FsCodec.StreamName.create name
    member _.TryDecode = FsCodec.StreamName.tryFind name >> ValueOption.map dec
