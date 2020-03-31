module Container

open Types
open FSharp.UMX

let [<Literal>] Category = "Container"

module Events =

    let streamName (containerId : string<containerId>) = FsCodec.StreamName.create Category (UMX.untag containerId)

    type Event =
        | ContainerFinalized
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

type Command = Finalize

let interpret (command: Command) (state : Fold.State): Events.Event list =
    match command with
    | Finalize -> [ if not state.finalized then yield Events.ContainerFinalized ]

type Service internal (resolve : string<containerId> -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(containerId, command : Command) : Async<unit> =
        let stream = resolve containerId
        stream.Transact(interpret command)
