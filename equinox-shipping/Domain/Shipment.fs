module Shipment

open System

type Shipment = { id: string; containerId: string }

module Events =

    let [<Literal>] CategoryId = "Shipment"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | ShipmentCreated    of shipmentId  : string
        | ShipmentAssigned   of containerId : string
        | ShipmentUnassigned of containerId : string
        | Snapshotted        of Shipment
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Shipment

    let initial: State = { id = null; containerId = null }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.Snapshotted      snapshot    -> snapshot
        | Events.ShipmentCreated  shipmentId  -> { state with id = shipmentId }
        | Events.ShipmentAssigned containerId -> { state with containerId = containerId }
        | Events.ShipmentUnassigned _         -> { state with containerId = null }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let isOrigin (event: Events.Event) =
        match event with
        | Events.Snapshotted _ -> true
        | _ -> false

    let snapshot state = Events.Snapshotted state

type Command =
    | Create   of shipmentId  : string
    | Assign   of containerId : string
    | Unassign of containerId : string

let interpret (command: Command) (state: Fold.State): bool * Events.Event list =
    match command with
    | Create shipmentId ->
        true, [ Events.ShipmentCreated  shipmentId ]
    | Assign containerId ->
        if String.IsNullOrWhiteSpace state.containerId then
            true, [ Events.ShipmentAssigned containerId ]
        else
            // Assignment fails if the shipment was already assigned.
            false, []
    | Unassign containerId ->
        true, [ Events.ShipmentUnassigned containerId ]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(shipment, command : Command) : Async<bool> =
        let stream = resolve shipment
        stream.Transact(interpret command)