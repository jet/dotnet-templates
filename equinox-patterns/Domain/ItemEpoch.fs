module Patterns.Domain.ItemEpoch

let [<Literal>] Category = "ItemEpoch"
let streamName (trancheId : ItemTrancheId, epochId : ItemEpochId) = FsCodec.StreamName.compose Category [ItemTrancheId.toString trancheId; ItemEpochId.toString epochId]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Ingested = { items : Item[] }
     and Item = { id : ItemId; payload : string }
    type Snapshotted = { ids : ItemId[]; closed : bool }
    type Event =
        | Ingested      of Ingested
        | Closed
        | Snapshotted   of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

let itemId (x : Events.Item) : ItemId = x.id
let (|ItemIds|) : Events.Item[] -> ItemId[] = Array.map itemId

module Fold =

    type State = ItemId[] * bool
    let initial = [||], false
    let evolve (ids, closed) = function
        | Events.Ingested { items = ItemIds ingestedIds } -> (Array.append ids ingestedIds, closed)
        | Events.Closed                                   -> (ids, true)
        | Events.Snapshotted e                            -> (e.ids, e.closed)
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (ids, closed) = Events.Snapshotted { ids = ids; closed = closed }

let notAlreadyIn (ids : ItemId seq) =
    let ids = System.Collections.Generic.HashSet ids
    fun (x : Events.Item) -> (not << ids.Contains) x.id

type Result = { accepted : ItemId[]; residual : Events.Item[]; content : ItemId[]; closed : bool }

// Note there aren't ever rejected Items in this implementation; the size of an epoch may actually exceed the capacity
// Pros for not rejecting:
// - snapshots should compress well
// - we want to avoid a second roundtrip
// - splitting a batched write into multiple writes with multiple events misrepresents the facts
//   i.e. we did not have 10 items 2s ago and 3 just now - we had 13 2s ago
let decide capacity candidates (currentIds, closed as state) =
    match closed, candidates |> Array.filter (notAlreadyIn currentIds) with
    | true, freshCandidates -> { accepted = [||]; residual = freshCandidates; content = currentIds; closed = closed }, []
    | false, [||] ->           { accepted = [||]; residual = [||];            content = currentIds; closed = closed }, []
    | false, freshItems ->
        let capacityNow = capacity freshItems currentIds
        let acceptingCount = min capacityNow freshItems.Length
        let closing = acceptingCount = capacityNow
        let ItemIds addedItemIds as itemsIngested, residualItems = Array.splitAt acceptingCount freshItems
        let events =
            [   if (not << Array.isEmpty) itemsIngested then yield Events.Ingested { items = itemsIngested }
                if closing then yield Events.Closed ]
        let currentIds, closed = Fold.fold state events
        { accepted = addedItemIds; residual = residualItems; content = currentIds; closed = closed }, events

/// Used by the Ingester to manages ingestion of items into the epoch, i.e. the Write side
type IngestionService internal (capacity, resolve : ItemTrancheId * ItemEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Obtains a complete list of all the items in the specified trancheId/epochId
    member _.ReadIds(trancheId, epochId) : Async<ItemId[]> =
        let decider = resolve (trancheId, epochId)
        decider.Query fst

    /// Ingest the supplied items. Yields relevant elements of the post-state to enable generation of stats
    /// and facilitate deduplication of incoming items in order to avoid null store round-trips where possible
    member _.Ingest(trancheId, epochId, items) : Async<Result> =
        let decider = resolve (trancheId, epochId)
        decider.Transact(decide capacity items)

let private create capacity resolveStream =
    let resolve = streamName >> resolveStream (Some Equinox.AllowStale) >> Equinox.createDecider
    IngestionService(capacity, resolve)

module MemoryStore =

    let create capacity store =
        let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create capacity resolveStream

module Cosmos =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create capacity (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create capacity resolveStream

/// Custom Fold and caching logic compared to the IngesterService
/// - When reading, we want the full Items
/// - Caching only for one minute
/// - There's no value in using the snapshot
module Reader =

    type ReadState = Events.Item[] * bool
    let initial = [||], false
    let evolve (es, closed as state) = function
        | Events.Ingested e    -> Array.append es e.items, closed
        | Events.Closed        -> (es, true)
        | Events.Snapshotted _ -> state // there's nothing useful in the snapshot for us to take
    let fold : ReadState -> Events.Event seq -> ReadState = Seq.fold evolve

    type Service internal (resolve : ItemTrancheId * ItemEpochId -> Equinox.Decider<Events.Event, ReadState>) =

        /// Returns all the items currently held in the stream
        member _.Read(trancheId, epochId) : Async<ReadState> =
            let decider = resolve (trancheId, epochId)
            decider.Query id

    let private create resolveStream =
        let resolve = streamName >> resolveStream >> Equinox.createDecider
        Service resolve

    module MemoryStore =

        let create store =
            let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, fold, initial)
            create cat.Resolve

    module Cosmos =

        open Equinox.CosmosStore

        let accessStrategy = AccessStrategy.Unoptimized
        let create (context, cache) =
            let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 1.)
            let cat = CosmosStoreCategory(context, Events.codec, fold, initial, cacheStrategy, accessStrategy)
            create cat.Resolve
