module Shipment

open Types
open FSharp.UMX

let [<Literal>] Category = "Shipment"

module Events =

    let streamName (shipmentId : string<shipmentId>) = FsCodec.StreamName.create Category (UMX.untag shipmentId)

    type Event =
        | ShipmentAssigned of {| containerId : string |}
        | ShipmentUnassigned
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = ShipmentState

    let initial: State = { association = None }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.ShipmentAssigned event -> { state with association = Some (UMX.tag event.containerId) }
        | Events.ShipmentUnassigned     -> { state with association = None }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

type Command =
    | Assign of containerId : string
    | Unassign

let interpret (command: Command) (state: Fold.State): bool * Events.Event list =
    match command with
    | Assign containerId ->
        match state.association with
        | Some _ ->
            // Assignment fails if the shipment was already assigned.
            false, []
        | None -> true, [ Events.ShipmentAssigned {| containerId = containerId |} ]

    | Unassign ->
        true, [ Events.ShipmentUnassigned ]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =
    member __.Execute(shipment, command : Command) : Async<bool> =
        let stream = resolve shipment
        stream.Transact(interpret command)