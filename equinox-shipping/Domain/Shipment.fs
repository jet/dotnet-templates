module Shipment

type ContainerId = string

type Shipment = { created: bool; association: ContainerId option }

module Events =

    let [<Literal>] CategoryId = "Shipment"
    let streamName clientId = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | ShipmentCreated
        | ShipmentAssigned of {| containerId : string |}
        | ShipmentUnassigned
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Shipment

    let initial: State = { created = false; container = None }

    let evolve (state: State) (event: Events.Event): State =
        match event with
        | Events.ShipmentCreated          -> { state with created = true }
        | Events.ShipmentAssigned   event -> { state with association = Some event.containerId }
        | Events.ShipmentUnassigned       -> { state with association = None }


    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

type Command =
    | Create
    | Assign   of containerId : string
    | Unassign

let interpret (command: Command) (state: Fold.State): bool * Events.Event list =
    match command with
    | Create ->
        true, [ if not state.created then yield Events.ShipmentCreated ]
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