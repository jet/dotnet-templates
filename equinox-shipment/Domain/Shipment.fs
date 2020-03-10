module Shipment

open System

type Shipment = { id: string; containerId: string }

module Events =

    let [<Literal>] CategoryId = "Shipment"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | ShipmentCreated    of shipmentId  : string
        | ShipmentAssigned   of containerId : string
        | ShipmentUnassigned
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
        | Events.ShipmentUnassigned           -> { state with containerId = null }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let isOrigin (event: Events.Event) =
        match event with
        | Events.Snapshotted _ -> true
        | _ -> false

    let snapshot state = Events.Snapshotted state

type Command =
    | Create of shipmentId  : string
    | Assign of containerId : string
    | Unassign

let interpret (command: Command) (state: Fold.State): Events.Event list =
    match command with
    | Create shipmentId ->
        [ Events.ShipmentCreated  shipmentId ]
    | Assign containerId ->
        [ if String.IsNullOrWhiteSpace state.containerId then yield Events.ShipmentAssigned containerId ]
    | Unassign ->
        [ Events.ShipmentUnassigned ]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(shipment, command : Command) : Async<unit> =
        let stream = resolve shipment
        stream.Transact(interpret command)