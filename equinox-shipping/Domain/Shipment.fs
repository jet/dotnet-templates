module Shipping.Domain.Shipment

let [<Literal>] private Category = "Shipment"
let streamName (shipmentId : ShipmentId) = FsCodec.StreamName.create Category (ShipmentId.toString shipmentId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        // TOCONSIDER: We could introduce a reserved state for a shipment if we needed to prevent a shipment being assigned twice
        | Assigned    of {| containerId : ContainerId |}
        | Revoked
        | Snapshotted of {| association : ContainerId option |}
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = { association : ContainerId option }
    let initial : State = { association = None }

    let evolve (_state: State) = function
        | Events.Assigned event -> { association = Some event.containerId }
        | Events.Revoked     -> { association = None }
        | Events.Snapshotted event      -> { association = event.association }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (state : State) = Events.Snapshotted {| association = state.association |}

type Command =
    | Assign
    | Revoke

let interpret containerId (command : Command) (state : Fold.State) : bool * Events.Event list =
    match command, state with
    | Assign, { association = None } ->
        true, [ Events.Assigned {| containerId = containerId |} ]
    | Assign, { association = Some current } when current <> containerId ->
        // Assignment fails if the shipment was already assigned to another container
        false, []
    | Assign, { association = _ } ->
        // Idempotently ignore
        true, []

    | Revoke, { association = Some current } when current <> containerId ->
        // The assignment is not ours to revoke -> Denied
        false, []
    | Revoke, { association = Some _ } ->
        true, [ Events.Revoked ]
    | Revoke, { association = _ } ->
        // Idempotently ignore
        true, []

type Service internal (resolve : ShipmentId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.TryAssign(shipmentId, containerId) : Async<bool> =
        let stream = resolve shipmentId
        stream.Transact(interpret containerId Assign)

    member __.TryRevoke(shipmentId, containerId) : Async<bool> =
        let stream = resolve shipmentId
        stream.Transact(interpret containerId Revoke)

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
