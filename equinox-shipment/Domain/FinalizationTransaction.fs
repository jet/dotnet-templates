module FinalizationTransaction

module Events =

    let [<Literal>] CategoryId = "FinalizationTransaction"

    let (|ForClientId|) (clientId: string) = FsCodec.StreamName.create CategoryId clientId

    type Event =
        | FinalizationRequested of shipmentIds: string[]
        | FinalizationReverted  of shipmentIds: string[]
        | FinalizationFailed
        | FinalizationCompleted
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

type Action =
    | RequestFinalization of shipmentIds: string[]
    | RevertFinalization  of shipmentIds: string[]
    | Finish of bool

module Fold =

    type State =
        | Initial
        | Running   of RunningState
        | Completed of bool
    and RunningState =
        | Assigning of shipmentIds: string[]
        | Reverting of shipmentIds: string[]

    let initial: State = Initial

    let evolve (_: State) (event: Events.Event): State =
        match event with
        | Events.FinalizationRequested shipmentIds -> Running (Assigning shipmentIds)
        | Events.FinalizationReverted  shipmentIds -> Running (Reverting shipmentIds)
        | Events.FinalizationFailed                -> Completed false
        | Events.FinalizationCompleted             -> Completed true

    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let filterValidTransition (event: Events.Event) (state: State) =
        match state, event with
        | Initial,               Events.FinalizationRequested _
        | Running (Assigning _), Events.FinalizationReverted _
        | Running (Assigning _), Events.FinalizationCompleted
        | Running (Reverting _), Events.FinalizationFailed ->
            Some event
        | _ -> None

    let nextAction (state: State): Action =
        match state with
        | Initial -> failwith "Cannot interpret Initial state"
        | Running (Assigning ids) -> Action.RequestFinalization ids
        | Running (Reverting ids) -> Action.RevertFinalization ids
        | Completed v             -> Action.Finish v

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