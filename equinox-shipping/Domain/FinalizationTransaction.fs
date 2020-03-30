module FinalizationTransaction

module Events =

    let [<Literal>] CategoryId = "FinalizationTransaction"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | FinalizationRequested of containerId: string * shipmentIds: string[]

        | AssignmentCompleted   of containerId: string * shipmentIds: string[]
        | FinalizationCompleted

        | RevertRequested       of shipmentIds: string[]
        | FinalizationFailed
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

type Action =
    | AssignShipments   of containerId: string * shipmentIds: string[]
    | FinalizeContainer of containerId: string * shipmentIds: string[]
    // Reverts the assignment of the container for the shipments provided.
    | RevertAssignment  of shipmentIds: string[]
    | Finish of bool

module Fold =

    type State =
        | Initial
        | Running   of RunningState
        | Completed of bool
    and RunningState =
        | Assigning of containerId: string * shipmentIds: string[]
        | Assigned  of containerId: string * shipmentIds: string[]
        | Reverting of shipmentIds: string[]

    let initial: State = Initial

    let evolve (_: State) (event: Events.Event): State =
        match event with
        | Events.FinalizationRequested (containerId, shipmentIds) -> Running (Assigning (containerId, shipmentIds))

        | Events.AssignmentCompleted   (containerId, shipmentIds) -> Running (Assigned  (containerId, shipmentIds))
        | Events.FinalizationCompleted                            -> Completed true

        | Events.RevertRequested       shipmentIds                -> Running (Reverting shipmentIds)
        | Events.FinalizationFailed                               -> Completed false

    let nextAction (state: State): Action =
        match state with
        | Initial -> failwith "Cannot interpret Initial state"
        | Running (Assigning (containerId, shipmentIds)) -> Action.AssignShipments   (containerId, shipmentIds)
        | Running (Assigned  (containerId, shipmentIds)) -> Action.FinalizeContainer (containerId, shipmentIds)
        | Running (Reverting shipmentIds)                -> Action.RevertAssignment  shipmentIds
        | Completed result                               -> Action.Finish result

    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let filterValidTransition (event: Events.Event) (state: State) =
        match state, event with
        | Initial,               Events.FinalizationRequested _
        | Running (Assigning _), Events.AssignmentCompleted   _
        | Running (Assigned _),  Events.FinalizationCompleted _
        | Running (Assigning _), Events.RevertRequested       _
        | Running (Reverting _), Events.FinalizationFailed    _ ->
            Some event
        | _ -> None

// When there are no event to apply to the state, it pushes the transaction manager to
// follow up on the next action from where it was.
let decide (update: Events.Event option) (state : Fold.State) : Action * Events.Event list =
    let events =
        update
        |> Option.bind (fun e -> Fold.filterValidTransition e state)
        |> Option.map  (fun e -> [e])
        |> Option.defaultValue []

    let state' =
        Fold.fold state events

    Fold.nextAction state', events

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)