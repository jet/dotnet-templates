module Patterns.Domain.ListEpoch

let [<Literal>] Category = "ListEpoch"
let streamName = ListEpochId.toString >> FsCodec.StreamName.create Category

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Ingested of           {| ids : ItemId[] |}
        | Closed
        | Snapshotted of        {| ids : ItemId[]; closed : bool |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = ItemId[] * bool
    let initial = [||], false
    let private evolve (ids, closed) = function
        | Events.Ingested e ->  Array.append e.ids ids, closed
        | Events.Closed ->      (ids, true)
        | Events.Snapshotted e -> (e.ids, e.closed)
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (ids, closed) = Events.Snapshotted {| ids = ids; closed = closed |}

type Result = { accepted : ItemId[]; closed : bool; residual : ItemId[] }

/// Manages ingestion of only items not already in the list
/// Yields residual net of items already present in this epoch
// NOTE See feedSource template for more advanced version handling splitting large input requests where epoch limit is strict 
let decide shouldClose candidateIds = function
    | currentIds, false as state ->
        let added, events =
            // TOCONSIDER in general, one would expect the inputs to naturally be distinct 
            match candidateIds |> Array.except currentIds (*|> Array.distinct*) with
            | [||] -> [||], []
            | news ->
                let closing = shouldClose news currentIds
                let ingestEvent = Events.Ingested {| ids = news |}
                news, if closing then [ ingestEvent ; Events.Closed ] else [ ingestEvent ]
        let state' = Fold.fold state events
        { accepted = added; closed = snd state'; residual = [||] }, events
    | currentIds, true ->
        { accepted = [||]; closed = true; residual = candidateIds |> Array.except currentIds (*|> Array.distinct*) }, []

// NOTE see feedSource for example of separating Service logic into Ingestion and Read Services in order to vary the folding and/or state held
type Service internal
    (   shouldClose : ItemId[] -> ItemId[] -> bool, // let outer layers decide whether ingestion should trigger closing of the batch
        resolve_ : Equinox.ResolveOption option -> ListEpochId -> Equinox.Decider<Events.Event, Fold.State>) =
    let resolve = resolve_ None
    let resolveStale = resolve_ (Some Equinox.AllowStale)
    
    /// Ingest the supplied items. Yields relevant elements of the post-state to enable generation of stats
    /// and facilitate deduplication of incoming items in order to avoid null store round-trips where possible
    member _.Ingest(epochId, items) : Async<Result> =
        let decider = resolveStale epochId
        /// NOTE decider which will initially transact against potentially stale cached state, which will trigger a
        /// resync if another writer has gotten in before us. This is a conscious decision in this instance; the bulk
        /// of writes are presumed to be coming from within this same process
        decider.Transact(decide shouldClose items)

    /// Returns all the items currently held in the stream (Not using AllowStale on the assumption this needs to see updates from other apps)
    member _.Read epochId : Async<Fold.State> =
        let decider = resolve epochId
        decider.Query id
            
let private create capacity resolveStream =
    let resolve opt = streamName >> resolveStream opt >> Equinox.createDecider
    Service(capacity, resolve)

module MemoryStore =

    let create capacity store =
        let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create capacity resolveStream

module Cosmos =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create shouldClose (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create shouldClose resolveStream
