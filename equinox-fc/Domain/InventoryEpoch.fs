/// Manages the ingestion (and deduplication based on a TransactionId) of events reflecting transfers or stock adjustments
///   that have been effected across a given set of Inventory
/// See Inventory.Service for surface level API which manages the ingestion, including transitioning to a new Epoch when an epoch reaches 'full' state
module Fc.Inventory.Epoch

let [<Literal>] Category = "InventoryEpoch"
let streamName (inventoryId, epochId) = FsCodec.StreamName.compose Category [InventoryId.toString inventoryId; InventoryEpochId.toString epochId]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type TransactionRef = { transactionId : InventoryTransactionId }
    type Snapshotted = { closed: bool; ids : InventoryTransactionId[] }

    type Event =
        | Adjusted    of TransactionRef
        | Transferred of TransactionRef
        | Closed
        | Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    /// Used for deduplicating input events
    let chooseInventoryTransactionId = function
       | Adjusted { transactionId = id } | Transferred { transactionId = id } -> Some id
       | Closed | Snapshotted _ -> None

module Fold =

    type State = { closed : bool; ids : Set<InventoryTransactionId> }
    let initial = { closed = false; ids = Set.empty }
    let evolve state = function
        | (Events.Adjusted e | Events.Transferred e) -> { state with ids = Set.add e.transactionId state.ids }
        | Events.Closed -> { state with closed = true }
        | Events.Snapshotted e -> { closed = e.closed; ids = Set.ofArray e.ids }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot s = Events.Snapshotted { closed = s.closed; ids = Array.ofSeq s.ids }

type Result =
    {   /// Indicates whether this epoch is closed (either previously or as a side-effect this time)
        isClosed : bool
        /// Count of items added to this epoch. May be less than requested due to removal of duplicates and/or rejected items
        added : int
        /// residual items that [are not duplicates and] were not accepted into this epoch
        rejected : Events.Event list
        /// identifiers for all items in this epoch
        transactionIds : Set<InventoryTransactionId> }

let decideSync capacity events (state : Fold.State) : Result * Events.Event list =
    let isFresh = function
        | Events.Adjusted { transactionId = id }
        | Events.Transferred { transactionId = id } -> (not << state.ids.Contains) id
        | Events.Closed | Events.Snapshotted _ -> false
    let news = events |> Seq.filter isFresh |> List.ofSeq
    let closed, allowing, markClosed, residual =
        let newCount = List.length news
        if state.closed then
            true, 0, false, news
        else
            let capacityNow = capacity state
            let accepting = min capacityNow newCount
            let closing = accepting = capacityNow
            let residual = List.skip accepting news
            closing, accepting, closing, residual
    let events =
        [   if allowing <> 0 then yield! news
            if markClosed then yield Events.Closed ]
    let state' = Fold.fold state events
    { isClosed = closed; added = allowing; rejected = residual; transactionIds = state'.ids }, events

type Service internal (resolve : InventoryId * InventoryEpochId -> Equinox.Stream<Events.Event, Fold.State>) =

    /// Attempt ingestion of `events` into the cited Epoch.
    /// - None will be accepted if the Epoch is `closed`
    /// - The `capacity` function will be passed a non-closed `state` in order to determine number of items that can be admitted prior to closing
    /// - If the computed capacity result is >= the number of items being submitted (which may be 0), the Epoch will be marked Closed
    /// NOTE the result may include rejected items (which the caller is expected to feed into a successor epoch)
    member __.TryIngest(inventoryId, epochId, capacity, events) : Async<Result> =
        let stream = resolve (inventoryId, epochId)
        stream.Transact(decideSync capacity events)

let create resolve =
    let resolve ids =
        let stream = resolve (streamName ids)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.snapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve

module EventStore =

    open Equinox.EventStore

    // No use of RollingSnapshots as we're intentionally making an epoch short enough to simply read any time
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy)
        create resolver.Resolve