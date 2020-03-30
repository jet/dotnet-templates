module Shipment

open System

type ContainerId = string

type Shipment = { container: ContainerId option }

module Events =

    let [<Literal>] CategoryId = "Shipment"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | ShipmentCreated  of shipmentId  : string
        | ShipmentAssigned of containerId : string
        | ShipmentUnassigned
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Shipment

    let initial: State = { container = None }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.ShipmentCreated    _           -> state
        | Events.ShipmentAssigned   containerId -> { state with container = Some containerId }
        | Events.ShipmentUnassigned _           -> { state with container = None }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

type Command =
    | Create   of shipmentId  : string
    | Assign   of containerId : string
    | Unassign

let interpret (command: Command) (state: Fold.State): bool * Events.Event list =
    match command with
    | Create shipmentId ->
        true, [ Events.ShipmentCreated shipmentId ]
    | Assign containerId ->
        match state.container with
        | Some _ ->
            // Assignment fails if the shipment was already assigned.
            false, []
        | None -> true, [ Events.ShipmentAssigned containerId ]

    | Unassign ->
        true, [ Events.ShipmentUnassigned ]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(shipment, command : Command) : Async<bool> =
        let stream = resolve shipment
        stream.Transact(interpret command)