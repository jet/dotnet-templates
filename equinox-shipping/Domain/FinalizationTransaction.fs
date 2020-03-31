module FinalizationTransaction

open Types
open FSharp.UMX

let [<Literal>] Category = "FinalizationTransaction"

type State =
    | Initial
    | Running   of RunningState
    | Completed of success : bool
and RunningState =
    | Assigning of shipmentIds : string<shipmentId>[]
    | Assigned  of shipmentIds : string<shipmentId>[]
    | Reverting of shipmentIds : string<shipmentId>[]

type FinalizationTransactionState = { container : string<containerId> option; state : State }

module Events =

    let streamName (transactionId : string<transactionId>) = FsCodec.StreamName.create Category (UMX.untag transactionId)

    type Event =
        | FinalizationRequested of {| containerId : string; shipmentIds : string[] |}
        | AssignmentCompleted   of {| shipmentIds : string[] |}
        | RevertRequested       of {| shipmentIds : string[] |}
        | Completed
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

type Action =
    | AssignShipments   of containerId : string<containerId> * shipmentIds : string<shipmentId>[]
    | FinalizeContainer of containerId : string<containerId> * shipmentIds : string<shipmentId>[]
    // Reverts the assignment of the container for the shipments provided.
    | RevertAssignment  of shipmentIds : string<shipmentId>[]
    | Finish            of success : bool

module Fold =

    type State = FinalizationTransactionState

    let initial: State = { container = None; state = Initial }

    let evolve (state : State) (event : Events.Event): State =
        let tag (ids: string[]) = ids |> Array.map UMX.tag

        match state, event with
        | _, Events.FinalizationRequested event ->
            { state with state = Running (Assigning (tag event.shipmentIds)) }

        | _, Events.AssignmentCompleted event ->
            { state with state = Running (Assigned (tag event.shipmentIds)) }

        | _, Events.RevertRequested event ->
            { state with state = Running (Reverting (tag event.shipmentIds)) }

        | { state = Running (Assigned _) }, Events.Completed ->
            { state with state = Completed true }

        | _, Events.Completed ->
            { state with state = Completed false }

    let nextAction (state: State): Action =
        match state with
        | { container = Some cId; state = Running (Assigning shipmentIds) } -> Action.AssignShipments   (cId, shipmentIds)
        | { container = Some cId; state = Running (Assigned  shipmentIds) } -> Action.FinalizeContainer (cId, shipmentIds)
        | { state = Running (Reverting shipmentIds) }                       -> Action.RevertAssignment shipmentIds
        | { state = Completed result }                                      -> Action.Finish result
        | s -> failwith (sprintf "Cannot interpret state %A" s)

    let fold: State -> Events.Event seq -> State =
        Seq.fold evolve

    let chooseValidTransition (event : Events.Event) (state : State) =
        match state, event with
        | { state = Initial },               Events.FinalizationRequested _
        | { state = Running (Assigning _) }, Events.AssignmentCompleted _
        | { state = Running (Assigning _) }, Events.RevertRequested _
        | { state = Running (Assigned _)  }, Events.Completed
        | { state = Running (Reverting _) }, Events.Completed -> Some event
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

type Service internal (resolve : string<transactionId> -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)