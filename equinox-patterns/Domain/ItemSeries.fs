/// Maintains a pointer into the Epoch chain for each Tranche
/// Allows an Ingester to quickly determine the current Epoch into which it should commence writing
/// As an Epoch is marked `Closed`, the Ingester will mark a new Epoch `Started` on this aggregate
module Patterns.Domain.ItemSeries

let [<Literal>] Category = "ItemSeries"
let streamName seriesId = FsCodec.StreamName.create Category (ItemSeriesId.toString seriesId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Started of {| trancheId : ItemTrancheId; epochId : ItemEpochId |}
        | Snapshotted of {| active : Map<ItemTrancheId, ItemEpochId> |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Map<ItemTrancheId, ItemEpochId>
    let initial = Map.empty
    let evolve state = function
        | Events.Started e -> state |> Map.add e.trancheId e.epochId
        | Events.Snapshotted e -> e.active
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = s |}

let tryFindEpochOfTranche : ItemTrancheId -> Fold.State -> ItemEpochId option = Map.tryFind

let interpret trancheId epochId (state : Fold.State) =
    [if state |> tryFindEpochOfTranche trancheId |> Option.forall (fun cur -> cur < epochId) && epochId >= ItemEpochId.initial then
        yield Events.Started {| trancheId = trancheId; epochId = epochId |}]

type Service internal (resolve_ : Equinox.ResolveOption option -> ItemSeriesId -> Equinox.Decider<Events.Event, Fold.State>, ?seriesId) =

    let resolveUncached = resolve_ None
    let resolveCached = resolve_ (Some Equinox.AllowStale)

    // For now we have a single global sequence. This provides us an extension point should we ever need to reprocess
    // NOTE we use a custom id in order to isolate data for acceptance tests
    let seriesId = defaultArg seriesId ItemSeriesId.wellKnownId

    /// Exposes the set of tranches for which data is held
    /// Never yields a cached value, in order to ensure a reader can traverse all tranches
    member _.Read() : Async<Fold.State> =
        let decider = resolveUncached seriesId
        decider.Query id

    /// Determines the current active epoch for the specified `trancheId`
    /// Uses cached values as epoch transitions are rare, and caller needs to deal with the inherent race condition in any case
    member _.TryReadIngestionEpochId trancheId : Async<ItemEpochId option> =
        let decider = resolveCached seriesId
        decider.Query(tryFindEpochOfTranche trancheId)

    /// Mark specified `epochId` as live for the purposes of ingesting
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing a successor via this
    member _.MarkIngestionEpochId(trancheId, epochId) : Async<unit> =
        let decider = resolveCached seriesId
        decider.Transact(interpret trancheId epochId)

let private create seriesOverride resolveStream =
    let resolve opt = streamName >> resolveStream opt >> Equinox.createDecider
    Service(resolve, ?seriesId = seriesOverride)

module Cosmos =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let private resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        fun opt sn -> resolver.Resolve(sn, ?option = opt)

    let create (context, cache) = create None (resolve (context, cache))
