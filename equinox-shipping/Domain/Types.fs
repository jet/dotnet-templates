module Types

open FSharp.UMX

[<Measure>] type shipmentId
[<Measure>] type containerId
[<Measure>] type transactionId

type ShipmentState = { association: string<containerId> option }

// Container state needs to be serializable as it will be stored as part of the Snapshotted event data.
type ContainerState = { finalized : bool }