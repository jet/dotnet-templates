module Container

// Container state needs to be serializable as it will be stored as part of the
// Snapshotted event data.
type ContainerState = { created: bool; finalized: bool }

module Events =

    let [<Literal>] CategoryId = "Shipment"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | ContainerCreated    of containerId  : string
        | ContainerFinalized  of shipmentIds  : string[]
        | Snapshotted         of ContainerState
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = ContainerState

    let initial: ContainerState = { created = false; finalized = false }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.Snapshotted        snapshot -> snapshot
        | Events.ContainerCreated   _ -> state
        | Events.ContainerFinalized _ -> { state with finalized = true }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let isOrigin (event: Events.Event) =
        match event with
        | Events.Snapshotted _ -> true
        | _ -> false

    let snapshot (state : State) = Events.Snapshotted state

type Command =
    | Create   of containerId : string
    | Finalize of shipmentIds : string[]

let interpret (command: Command) (state: Fold.State): Events.Event list =
    match command with
    | Create containerId  ->
        [ Events.ContainerCreated containerId ]
    | Finalize shipmentIds  ->
        [ if not state.finalized then yield Events.ContainerFinalized shipmentIds ]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(shipment, command : Command) : Async<unit> =
        let stream = resolve shipment
        stream.Transact(interpret command)
