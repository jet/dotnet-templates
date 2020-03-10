module Container

type Container = { id: string; shipmentIds: string[] }

module Events =

    let [<Literal>] CategoryId = "Shipment"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | ContainerCreated    of containerId  : string
        | ContainerFinalized  of shipmentIds  : string[]
        | Snapshotted         of Container
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Container

    let initial: State = { id = null; shipmentIds = [||] }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.Snapshotted        snapshot    -> snapshot
        | Events.ContainerCreated   containerId -> { state with id = containerId }
        | Events.ContainerFinalized shipmentIds -> { state with shipmentIds = shipmentIds }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let isOrigin (event: Events.Event) =
        match event with
        | Events.Snapshotted _ -> true
        | _ -> false

    let snapshot state = Events.Snapshotted state

type Command =
    | Create   of containerId : string
    | Finalize of shipmentIds : string[]

let interpret (command: Command) (state: Fold.State): Events.Event list =
    match command with
    | Create   containerId  -> [ Events.ContainerCreated   containerId ]
    | Finalize shipmentIds  -> [ Events.ContainerFinalized shipmentIds ]
