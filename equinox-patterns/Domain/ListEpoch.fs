module Patterns.Domain.ListEpoch

module private Stream =
    let [<Literal>] Category = "ListEpoch"
    let id = Equinox.StreamId.gen ListEpochId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Ingested of           {| ids: ItemId[] |}
        | Closed
        | Snapshotted of        {| ids: ItemId[]; closed: bool |}
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Fold =

    type State = ItemId[] * bool
    let initial = [||], false
    let private evolve (ids, closed) = function
        | Events.Ingested e ->  Array.append e.ids ids, closed
        | Events.Closed ->      (ids, true)
        | Events.Snapshotted e -> (e.ids, e.closed)
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (ids, closed) = Events.Snapshotted {| ids = ids; closed = closed |}

/// Manages ingestion of only items not already in the list
/// Yields residual net of items already present in this epoch
// NOTE See feedSource template for more advanced version handling splitting large input requests where epoch limit is strict
let decide shouldClose candidateIds = function
    | currentIds, false as state ->
        let added, events =
            // TOCONSIDER in general, one would expect the inputs to naturally be distinct
            match candidateIds |> Array.except currentIds (*|> Array.distinct*) with
            | [||] -> [||], [||]
            | news ->
                let closing = shouldClose news currentIds
                let ingestEvent = Events.Ingested {| ids = news |}
                news, [| ingestEvent; if closing then Events.Closed |]
        let _, closed = Fold.fold state events
        let res: ExactlyOnceIngester.IngestResult<_, _> = { accepted = added; closed = closed; residual = [||] }
        res, events
    | currentIds, true ->
        { accepted = [||]; closed = true; residual = candidateIds |> Array.except currentIds (*|> Array.distinct*) }, [||]

// NOTE see feedSource for example of separating Service logic into Ingestion and Read Services in order to vary the folding and/or state held
type Service internal
    (   shouldClose: ItemId[] -> ItemId[] -> bool, // let outer layers decide whether ingestion should trigger closing of the batch
        resolve: ListEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Ingest the supplied items. Yields relevant elements of the post-state to enable generation of stats
    /// and facilitate deduplication of incoming items in order to avoid null store round-trips where possible
    member _.Ingest(epochId, items): Async<ExactlyOnceIngester.IngestResult<_, _>> =
        let decider = resolve epochId
        // NOTE decider which will initially transact against potentially stale cached state, which will trigger a
        // resync if another writer has gotten in before us. This is a conscious decision in this instance; the bulk
        // of writes are presumed to be coming from within this same process
        decider.Transact(decide shouldClose items, load = Equinox.AnyCachedValue)

    /// Returns all the items currently held in the stream (Not using AnyCachedValue on the assumption this needs to see updates from other apps)
    member _.Read epochId: Async<Fold.State> =
        let decider = resolve epochId
        decider.Query(id, Equinox.AllowStale (System.TimeSpan.FromSeconds 1))

module Factory =

    let private (|Category|) = function
        | Store.Context.Memory store -> Store.Memory.create Stream.Category Events.codec Fold.initial Fold.fold store
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Stream.Category Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
    let create maxItemsPerEpoch (Category cat) =
        let shouldClose candidateItems currentItems = Array.length currentItems + Array.length candidateItems >= maxItemsPerEpoch
        Service(shouldClose, Stream.id >> Store.createDecider cat)
