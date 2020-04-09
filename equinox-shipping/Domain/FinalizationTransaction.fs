module Shipping.Domain.FinalizationTransaction

let [<Literal>] private Category = "FinalizationTransaction"
let streamName (transactionId : TransactionId) = FsCodec.StreamName.create Category (TransactionId.toString transactionId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | FinalizationRequested of {| containerId : ContainerId; shipmentIds : ShipmentId[] |}
        | AssignmentCompleted
        /// Signifies we're switching focus to relinquishing any assignments we completed.
        /// The list includes any items we could possibly have touched (including via idempotent retries)
        | RevertRequested       of {| shipmentIds : ShipmentId[] |}
        | Completed
        | Snapshotted           of State
        interface TypeShape.UnionContract.IUnionContract

    and [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.UnionConverter>)>]
        State =
        | Initial
        | Assigning of TransactionState
        | Assigned  of containerId : ContainerId
        | Reverting of TransactionState
        | Completed of success : bool
    and TransactionState = { container : ContainerId; shipments : ShipmentId[] }

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

open Events

let initial : State = Initial

let isValidTransition (event : Events.Event) (state : State) =
    match state, event with
    | Initial,     FinalizationRequested _
    | Assigning _, AssignmentCompleted _
    | Assigning _, RevertRequested _
    | Assigned _,  Events.Completed
    | Reverting _, Events.Completed -> true
    | _ -> false

// The implementation trusts (does not spend time double checking) that events have passed an isValidTransition check
let evolve (state : State) (event : Event) : State =
    match state, event with
    | _, FinalizationRequested event         -> Assigning { container = event.containerId; shipments = event.shipmentIds }
    | Assigning state, AssignmentCompleted   -> Assigned state.container
    | Assigning state, RevertRequested event -> Reverting { state with shipments = event.shipmentIds }
    | Assigned, Event.Completed              -> Completed true
    | Reverting _state, Event.Completed      -> Completed false
    | _, Snapshotted state                   -> state
    | state, _                               -> state
let fold : State -> Events.Event seq -> State = Seq.fold evolve

let isOrigin = function Events.Snapshotted -> true | _ -> false
let toSnapshot state = Events.Snapshotted state

type Action =
    | AssignShipments   of containerId : ContainerId * shipmentIds : ShipmentId []
    | FinalizeContainer of containerId : ContainerId
    | RevertAssignment  of containerId : ContainerId * shipmentIds : ShipmentId []
    | Finish            of success : bool

let nextAction : State -> Action = function
    | Assigning state      -> AssignShipments   (state.container, state.shipments)
    | Assigned containerId -> FinalizeContainer containerId
    | Reverting state      -> RevertAssignment  (state.container, state.shipments)
    | Completed result     -> Finish result
    | Initial as s         -> failwith (sprintf "Cannot interpret state %A" s)

// When there are no event to apply to the state, it pushes the transaction manager to
// follow up on the next action from where it was.
let decide (update : Events.Event option) (state : State) : Action * Events.Event list =
    let events =
        match update with
        | Some e when isValidTransition e state -> [e]
        | _ -> []

    let state' = fold state events
    nextAction state', events

type Service internal (resolve : TransactionId -> Equinox.Stream<Events.Event, State>) =

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)

let private create resolve =
    let resolve id = Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve (streamName id), maxAttempts = 3)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, fold, initial, cacheStrategy, accessStrategy)
        create resolver.Resolve