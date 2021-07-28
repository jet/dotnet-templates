/// Maintains a pointer into the Epoch chain
/// Allows the Ingester to quickly determine the current Epoch into which it should commence writing
/// As an Epoch is marked `Closed`, the Ingester will mark a new Epoch `Started` on this aggregate
module Patterns.Domain.ItemSeries

let [<Literal>] Category = "ItemSeries"
let streamName = ItemSeriesId.toString >> FsCodec.StreamName.create Category

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Started       of {| epochId : ItemEpochId |}
        | Snapshotted   of {| active : ItemEpochId option |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = ItemEpochId option
    let initial = None
    let evolve _state = function
        | Events.Started e     -> Some e.epochId
        | Events.Snapshotted e -> e.active
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = s |}

let interpret epochId (state : Fold.State) =
    [if state |> Option.forall (fun cur -> cur < epochId) && epochId >= ItemEpochId.initial then
        yield Events.Started {| epochId = epochId |}]

type Service internal (resolve_ : Equinox.ResolveOption option -> ItemSeriesId -> Equinox.Decider<Events.Event, Fold.State>, ?seriesId) =

    let resolve = resolve_ None
    let resolveStale = resolve_ (Some Equinox.AllowStale)

    // For now we have a single global sequence. This provides us an extension point should we ever need to reprocess
    // NOTE we use a custom id in order to isolate data for acceptance tests
    let seriesId = defaultArg seriesId ItemSeriesId.wellKnownId

    /// Determines the current active epoch
    /// Uses cached values as epoch transitions are rare, and caller needs to deal with the inherent race condition in any case
    member _.ReadIngestionEpochId() : Async<ItemEpochId> =
        let decider = resolve seriesId
        decider.Query(Option.defaultValue ItemEpochId.initial)

    /// Mark specified `epochId` as live for the purposes of ingesting
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing a successor via this
    member _.MarkIngestionEpochId(epochId) : Async<unit> =
        let decider = resolveStale seriesId
        decider.Transact(interpret epochId)

let private create seriesOverride resolveStream =
    let resolve opt = streamName >> resolveStream opt >> Equinox.createDecider
    Service(resolve, ?seriesId = seriesOverride)

module MemoryStore =

    let create store =
        let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create None resolveStream

module Cosmos =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create None resolveStream
