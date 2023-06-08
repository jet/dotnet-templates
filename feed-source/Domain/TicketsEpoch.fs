/// Tracks all Tickets that entered the system over a period of time
/// - Used to walk back through the history of all tickets in the system in approximate order of their processing
/// - Limited to a certain reasonable count of items; snapshot of Tickets in an epoch needs to stay a sensible size
/// The TicketsSeries holds a pointer to the current active epoch for each FC
/// Each successive epoch is identified by an index, i.e. TicketsEpoch-FC001_0, then TicketsEpoch-FC001_1
module FeedSourceTemplate.Domain.TicketsEpoch

let [<Literal>] Category = "TicketsEpoch"
let streamId = Equinox.StreamId.gen2 FcId.toString TicketsEpochId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Ingested = { items: Item[] }
     and Item = { id: TicketId; payload: string }
    type Event =
        | Ingested    of Ingested
        | Closed
        | Snapshotted of {| ids: TicketId[]; closed: bool |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>

let itemId (x: Events.Item): TicketId = x.id
let (|ItemIds|): Events.Item[] -> TicketId[] = Array.map itemId

module Fold =

    type State = TicketId[] * bool
    let initial = [||], false
    let evolve (ids, closed) = function
        | Events.Ingested { items = ItemIds ingestedIds } -> (Array.append ids ingestedIds, closed)
        | Events.Closed                                   -> (ids, true)
        | Events.Snapshotted e                            -> (e.ids, e.closed)

    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (ids, closed) = Events.Snapshotted {| ids = ids; closed = closed |}

let notAlreadyIn (ids: TicketId seq) =
    let ids = System.Collections.Generic.HashSet ids
    fun (x: Events.Item) -> (not << ids.Contains) x.id

type Result = { accepted: TicketId[]; residual: Events.Item[]; content: TicketId[]; closed: bool }

/// NOTE See eqxPatterns template ItemEpoch for a simplified decide function which does not split ingestion requests with a rigid capacity rule
/// NOTE does not deduplicate (distinct) candidates and/or the residuals on the basis that the caller should guarantee that
let decide capacity candidates (currentIds, closed as state) =
    match closed, candidates |> Array.filter (notAlreadyIn currentIds) with
    | true, freshCandidates -> { accepted = [||]; residual = freshCandidates; content = currentIds; closed = closed }, []
    | false, [||] ->           { accepted = [||]; residual = [||];            content = currentIds; closed = closed }, []
    | false, freshItems ->
        // NOTE we in some cases end up triggering splitting of a request (or set of requests coalesced in the Batcher)
        // In some cases it might be better to be a little tolerant and not be rigid about limiting things as
        // - snapshots should compress well (no major incremental cost for a few more items)
        // - its always good to avoid a second store roundtrip
        // - splitting a batched write into multiple writes with multiple events misrepresents the facts i.e. we did
        //   not have 10 items 2s ago and 3 just now - we had 13 2s ago
        let capacityNow = capacity freshItems currentIds
        let acceptingCount = min capacityNow freshItems.Length
        let closing = acceptingCount = capacityNow
        let ItemIds addedItemIds as itemsIngested, residualItems = Array.splitAt acceptingCount freshItems
        let events =
            [   if (not << Array.isEmpty) itemsIngested then yield Events.Ingested { items = itemsIngested }
                if closing then yield Events.Closed ]
        let currentIds, closed = Fold.fold state events
        { accepted = addedItemIds; residual = residualItems; content = currentIds; closed = closed }, events

/// Service used for the write side; manages ingestion of items into the series of epochs
type IngestionService internal (capacity, resolve: FcId * TicketsEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Handles idempotent deduplicated insertion into the set of items held within the epoch
    member _.Ingest(fcId, epochId, ticketIds): Async<Result> =
        let decider = resolve (fcId, epochId)
        // Accept whatever date is in the cache on the basis that we are doing most of the writing so will more often than not
        // have the correct state already without a roundtrip. What if the data is actually stale? we'll end up needing to resync,
        // but we we need to deal with that as a race condition anyway
        decider.Transact(decide capacity ticketIds, Equinox.AnyCachedValue)

    /// Obtains a complete list of all the tickets in the specified fcid/epochId
    /// NOTE AnyCachedValue option assumes that it's safe to ignore writes from other nodes
    member _.ReadTickets(fcId, epochId): Async<TicketId[]> =
        let decider = resolve (fcId, epochId)
        decider.Query(fst, Equinox.AnyCachedValue)

module Factory =

    let private create_ capacity resolve =
        IngestionService(capacity, streamId >> resolve)
    let private (|Category|) = function
        | Store.Context.Memory store ->            Store.Memory.create Events.codec Fold.initial Fold.fold store
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
    let create capacity (Category cat) = Store.createDecider cat Category |> create_ capacity

/// Custom Fold and caching logic compared to the IngesterService
/// - When reading, we want the full Items
/// - Caching only for one minute
/// - There's no value in using the snapshot as it does not have the full state
module Reader =

    type State = Events.Item[] * bool
    let initial = [||], false
    let evolve (es, closed as state) = function
        | Events.Ingested e    -> Array.append es e.items, closed
        | Events.Closed        -> (es, true)
        | Events.Snapshotted _ -> state // there's nothing useful in the snapshot for us to take
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    type StateDto = { closed: bool; tickets: Events.Item[] }

    type Service internal (resolve: FcId * TicketsEpochId -> Equinox.Decider<Events.Event, State>) =

        /// Returns all the items currently held in the stream
        member _.Read(fcId, epochId): Async<StateDto> =
            let decider = resolve (fcId, epochId)
            decider.Query(fun (items, closed) -> { closed = closed; tickets = items })

    module Factory =

        let private (|Category|) = function
            | Store.Context.Memory store ->            Store.Memory.create Events.codec initial fold store
            | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createUnoptimized Events.codec initial fold (context, cache)
        let create (Category cat) = Service(streamId >> Store.createDecider cat Category)
