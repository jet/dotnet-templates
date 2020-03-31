module FinalizationTransaction

open Types
open FSharp.UMX

let [<Literal>] Category = "FinalizationTransaction"

module Events =

    let streamName (transactionId : string<transactionId>) = FsCodec.StreamName.create Category (UMX.untag transactionId)

    type Event =
        | FinalizationRequested of {| containerId : string; shipmentIds : string[] |}
        | AssignmentCompleted   of {| containerId : string; shipmentIds : string[] |}
        | RevertRequested       of {| shipmentIds : string[] |}
        | Completed
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

type Action =
    | AssignShipments   of containerId : string * shipmentIds : string[]
    | FinalizeContainer of containerId : string * shipmentIds : string[]
    // Reverts the assignment of the container for the shipments provided.
    | RevertAssignment  of shipmentIds : string[]
    | Finish            of success : bool

module Fold =

    type State =
        | Initial
        | Running   of RunningState
        | Completed of success : bool
    and RunningState =
        | Assigning of containerId : string * shipmentIds : string[]
        | Assigned  of containerId : string * shipmentIds : string[]
        | Reverting of shipmentIds : string[]

    let initial: State = Initial

    let evolve (state : State) (event : Events.Event): State =
        match state, event with
        | _, Events.FinalizationRequested event  -> Running (Assigning (event.containerId, event.shipmentIds))
        | _, Events.AssignmentCompleted   event  -> Running (Assigned  (event.containerId, event.shipmentIds))
        | _, Events.RevertRequested       event  -> Running (Reverting event.shipmentIds)
        | Running (Assigned _), Events.Completed -> Completed true
        | _,                    Events.Completed -> Completed false

    let nextAction (state: State): Action =
        match state with
        | Initial -> failwith "Cannot interpret Initial state"
        | Running (Assigning (containerId, shipmentIds)) -> Action.AssignShipments   (containerId, shipmentIds)
        | Running (Assigned  (containerId, shipmentIds)) -> Action.FinalizeContainer (containerId, shipmentIds)
        | Running (Reverting shipmentIds)                -> Action.RevertAssignment  shipmentIds
        | Completed result                               -> Action.Finish result

    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let chooseValidTransition (event : Events.Event) (state : State) =
        match state, event with
        | Initial,               Events.FinalizationRequested _
        | Running (Assigning _), Events.AssignmentCompleted _
        | Running (Assigning _), Events.RevertRequested _
        | Running (Assigned _),  Events.Completed
        | Running (Reverting _), Events.Completed -> Some event
        | _ -> None

// When there are no event to apply to the state, it pushes the transaction manager to
// follow up on the next action from where it was.
let decide (update : Events.Event option) (state : Fold.State) : Action * Events.Event list =
    let events =
        match update with
        | Some e -> Fold.chooseValidTransition e state |> Option.toList
        | None   -> []

    let state' =
        Fold.fold state events

    Fold.nextAction state', events

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)