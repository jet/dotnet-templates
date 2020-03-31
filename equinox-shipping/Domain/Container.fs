module Container

open Types
open FSharp.UMX

let [<Literal>] Category = "Container"

module Events =

    let streamName (containerId : string<containerId>) = FsCodec.StreamName.create Category (UMX.untag containerId)

    type Event =
        | ContainerFinalized  of {| shipmentIds: string[] |}
        | Snapshotted         of ContainerState
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = ContainerState

    let initial: ContainerState = { finalized = false }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.Snapshotted snapshot -> snapshot
        | Events.ContainerFinalized _ -> { state with finalized = true }

    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let isOrigin (event: Events.Event) =
        match event with
        | Events.Snapshotted _ -> true
        | _ -> false

    let snapshot (state : State) = Events.Snapshotted state

type Command = Finalize of shipmentIds : string[]

let interpret (Finalize shipmentIds) (state : Fold.State): Events.Event list =
    [ if not state.finalized then yield Events.ContainerFinalized {| shipmentIds = shipmentIds |} ]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(shipment, command : Command) : Async<unit> =
        let stream = resolve shipment
        stream.Transact(interpret command)
