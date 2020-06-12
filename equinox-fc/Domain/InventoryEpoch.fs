/// Manages the ingestion (and deduplication based on a TransactionId) of events reflecting transfers or stock adjustments
///   that have been effected across a given set of Inventory
/// See Inventory.Service for surface level API which manages the ingestion
module Fc.Domain.Inventory.Epoch

let [<Literal>] Category = "InventoryEpoch"
let streamName inventoryId = FsCodec.StreamName.compose Category [InventoryId.toString inventoryId; "0"]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type TransactionRef = { transactionId : InventoryTransactionId }

    type Event =
        | Adjusted    of TransactionRef
        | Transferred of TransactionRef
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    /// Used for deduplicating input events
    let chooseInventoryTransactionId = function
       | Adjusted { transactionId = id } | Transferred { transactionId = id } -> Some id

module Fold =

    type State = { closed : bool; ids : Set<InventoryTransactionId> }
    let initial = { closed = false; ids = Set.empty }
    let evolve state = function
        | (Events.Adjusted e | Events.Transferred e) -> { state with ids = Set.add e.transactionId state.ids }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

type Result =
    {   /// Count of items added to this epoch. May be less than requested due to removal of duplicates and/or rejected items
        added : int
        /// identifiers for all items in this epoch
        transactionIds : Set<InventoryTransactionId> }

let decideSync events (state : Fold.State) : Result * Events.Event list =
    let isFresh = function
        | Events.Adjusted { transactionId = id }
        | Events.Transferred { transactionId = id } -> (not << state.ids.Contains) id
    let news = events |> Seq.filter isFresh |> List.ofSeq
    let newCount = List.length news
    let events = [ if newCount <> 0 then yield! news ]
    let state' = Fold.fold state events
    { added = newCount; transactionIds = state'.ids }, events

type Service internal (resolve : InventoryId -> Equinox.Stream<Events.Event, Fold.State>) =

    /// Attempt ingestion of `events` into the cited Epoch.
    /// - None will be accepted if the Epoch is `closed`
    /// - The `capacity` function will be passed a non-closed `state` in order to determine number of items that can be admitted prior to closing
    /// - If the computed capacity result is >= the number of items being submitted (which may be 0), the Epoch will be marked Closed
    /// NOTE the result may include rejected items (which the caller is expected to feed into a successor epoch)
    member __.TryIngest(inventoryId, events) : Async<Result> =
        let stream = resolve inventoryId
        stream.Transact(decideSync events)

let create resolve =
    let resolve ids =
        let stream = resolve (streamName ids)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts=2)
    Service(resolve)

module EventStore =

    open Equinox.EventStore

    // No use of RollingSnapshots as we're intentionally making an epoch short enough to simply read any time
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy)
        create resolver.Resolve