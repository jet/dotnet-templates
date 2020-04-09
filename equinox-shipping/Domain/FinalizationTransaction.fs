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
        | RevertCommenced       of {| shipmentIds : ShipmentId[] |}
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

module Fold =

    type State = Events.State
    let initial : State = State.Initial

    let isValidTransition (event : Events.Event) (state : State) =
        match state, event with
        | State.Initial,     Events.FinalizationRequested _
        | State.Assigning _, Events.AssignmentCompleted _
        | State.Assigning _, Events.RevertCommenced _
        | State.Assigned _,  Events.Completed
        | State.Reverting _, Events.Completed -> true
        | _ -> false

    // The implementation trusts (does not spend time double checking) that events have passed an isValidTransition check
    let evolve (state : State) (event : Events.Event) : State =
        match state, event with
        | _, Events.FinalizationRequested event                -> State.Assigning { container = event.containerId; shipments = event.shipmentIds }
        | State.Assigning state,  Events.AssignmentCompleted   -> State.Assigned state.container
        | State.Assigning state,  Events.RevertCommenced event -> State.Reverting { state with shipments = event.shipmentIds }
        | State.Assigned,         Events.Completed             -> State.Completed true
        | State.Reverting _state, Events.Completed             -> State.Completed false
        | _,                      Events.Snapshotted state     -> state
        // this shouldn't happen, but, if we did produce invalid events, we'll just ignore them
        | state, _                                             -> state
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted -> true | _ -> false
    let toSnapshot state = Events.Snapshotted state

type Action =
    | AssignShipments   of containerId : ContainerId * shipmentIds : ShipmentId []
    | FinalizeContainer of containerId : ContainerId
    | RevertAssignment  of containerId : ContainerId * shipmentIds : ShipmentId []
    | Finish            of success : bool

let nextAction : Fold.State -> Action = function
    | Fold.State.Assigning state      -> AssignShipments   (state.container, state.shipments)
    | Fold.State.Assigned containerId -> FinalizeContainer containerId
    | Fold.State.Reverting state      -> RevertAssignment  (state.container, state.shipments)
    | Fold.State.Completed result     -> Finish result
    // As all state transitions are driven by members on the FinalizationProcessManager, we can rule this out
    | Fold.State.Initial as s         -> failwith (sprintf "Cannot interpret state %A" s)

// If there are no events to apply to the state, it pushes the transaction manager to
// follow up on the next action from where it was.
let decide (update : Events.Event option) (state : Fold.State) : Action * Events.Event list =
    let events =
        match update with
        | Some e when Fold.isValidTransition e state -> [e]
        | _ -> []

    let state' = Fold.fold state events
    nextAction state', events

type Service internal (resolve : TransactionId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Apply(transactionId, update) : Async<Action> =
        let stream = resolve transactionId
        stream.Transact(decide update)

let private create resolve =
    let resolve id = Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve (streamName id), maxAttempts = 3)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
