module Shipping.Domain.Shipment

let [<Literal>] private Category = "Shipment"
let streamName (shipmentId : ShipmentId) = FsCodec.StreamName.create Category (ShipmentId.toString shipmentId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Reserved    of {| transaction : TransactionId |}
        | Assigned    of {| container : ContainerId |}
        | Revoked
        | Snapshotted of {| reservation : TransactionId option; association : ContainerId option |}
        interface TypeShape.UnionContract.IUnionContract

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = { reservation : TransactionId option; association : ContainerId option }
    let initial : State = { reservation = None; association = None }

    let evolve (state: State) = function
        | Events.Reserved event     -> { reservation = Some event.transaction; association = None }
        | Events.Revoked            ->   initial
        | Events.Assigned event     -> { state with                            association = Some event.container  }
        | Events.Snapshotted event  -> { reservation = event.reservation;      association = event.association }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (state : State) = Events.Snapshotted {| reservation = state.reservation; association = state.association |}

let decideReserve transactionId : Fold.State -> bool * Events.Event list = function
    | { reservation = r; association = a } ->
        // validity is dependent on whether its a) open or b) an idempotent retry by the same transaction
        let isValid = Option.forall (fun x -> x = transactionId) r
        // if it's already Reserved (and/or assigned), there's no work to do
        isValid, if isValid && Option.isNone a then [ Events.Reserved {| transaction = transactionId |} ] else []

let interpretRevoke transactionId : Fold.State -> Events.Event list = function
    | { reservation = Some current; association = None } when current = transactionId ->
        [ Events.Revoked ]
    | _ -> [] // Ignore if a) already revoked/never reserved b) not reserved for this transactionId

let interpretAssign transactionId containerId : Fold.State -> Events.Event list = function
    | { reservation = Some r; association = None } when r = transactionId ->
        [ Events.Assigned {| container = containerId |} ]
    | _ -> [] // Ignore if a) this transaction was not the one reserving it or b) it's already been assigned

type Service internal (resolve : ShipmentId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.TryReserve(shipmentId, transactionId) : Async<bool> =
        let stream = resolve shipmentId
        stream.Transact(decideReserve transactionId)

    member __.Revoke(shipmentId, transactionId) : Async<unit> =
        let stream = resolve shipmentId
        stream.Transact(interpretRevoke transactionId)

    member __.Assign(shipmentId, containerId, transactionId) : Async<unit> =
        let stream = resolve shipmentId
        stream.Transact(interpretAssign transactionId containerId)

let private create resolve =
    let resolve id = Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve (streamName id), maxAttempts=3)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
