module Store

module Metrics = 

    let [<Literal>] PropertyTag = "isMetric"
    let log = Serilog.Log.ForContext(PropertyTag, true)

let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
       FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() // options = Options.Default

/// Implements a Service with a single method that visits the identified stream, with the following possible outcomes:
/// 1) stream has a 'current' snapshot (per the `isCurrentSnapshot` predicate supplied to `Snapshot.create` and/or `fold'`:-
///   - no writes take place
///   - state and version enter cache (not strictly necessary; but would enable purging of state to have a reduced effect in terms of inducing redundant loads)
///   - no further invocations should be triggered until there is a fresh event
/// 2) stream state was derived by either loading and folding all events:-
///   - pretend we are writing an event so we trigger a Sync operation
///   - The `transmuteAllEventsToUnfolds` that was supplied to `AccessStrategy.Custom`:
///      - replaces this placeholder 'event' with an empty Array of events
///      - flips the snapshot event into the 'unfolds' list
///   - The Sync Stored procedure then processes the ensuing request, replacing the current (missing or outdated) `'u`nfolds with the fresh snapshot
module Snapshotter =

    type private StateWithSnapshottedFlag<'s> = bool * 's
    type Service<'id, 'e, 's> internal (resolve: 'id -> Equinox.Decider<'e, StateWithSnapshottedFlag<'s>>, generate: 's -> 'e) =

        member _.TryUpdate(id): Async<bool * int64> =
            let decider = resolve id
            let decide (hasSnapshot, state) =
                if hasSnapshot then false, Array.empty // case 1: no update required as the stream already has a correct snapshot
                else true, generate state |> Array.singleton // case 2: yield a tentative event (which transmuteAllEventsToUnfolds will flip to being an unfold)
            decider.TransactWithPostVersion(decide)

    let internal createService streamId generate cat =
        let resolve = streamId >> createDecider cat
        Service(resolve, generate)

    let internal initial'<'s> initial: StateWithSnapshottedFlag<'s> = false, initial
    let internal fold' isCurrentSnapshot fold (_wasOrigin, s) xs: StateWithSnapshottedFlag<'s> =
        // NOTE ITimelineEvent.IsUnfold and/or a generic isOrigin event would be insufficient for our needs
        // The tail event encountered by the fold could either be:
        // - an 'out of date' snapshot (which the normal load process would be able to upconvert from, but is not what we desire)
        // - another event (if there is no snapshot of any kind)
        isCurrentSnapshot (Array.last xs), fold s xs
        
let private defaultCacheDuration = System.TimeSpan.FromMinutes 20
let private cacheStrategy cache = Equinox.CachingStrategy.SlidingWindow (cache, defaultCacheDuration)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type Config =
    | Cosmos of contexts: CosmosContexts * cache: Equinox.Cache
and [<NoComparison; NoEquality>] CosmosContexts =
    { main: Equinox.CosmosStore.CosmosStoreContext
      views: Equinox.CosmosStore.CosmosStoreContext
      /// Variant of `main` that's configured such that `module Snapshotter` updates will never trigger a calve
      snapshotUpdate: Equinox.CosmosStore.CosmosStoreContext }

module Cosmos =

    open Equinox.CosmosStore
    
    let private createCached name codec initial fold accessStrategy (context, cache) =
        CosmosStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (context.main, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy (context.views, cache)

    let createConfig (main, views, snapshotUpdate) cache =
        Config.Cosmos ({ main = main; views = views; snapshotUpdate = snapshotUpdate }, cache)

    module Snapshotter =

        let private accessStrategy isOrigin =
            let transmuteAllEventsToUnfolds events _state = [||], events
            AccessStrategy.Custom (isOrigin, transmuteAllEventsToUnfolds)
        let private createCategory name codec initial fold isCurrent (contexts, cache) =
            createCached name codec (Snapshotter.initial' initial) (Snapshotter.fold' isCurrent fold) (accessStrategy isCurrent) (contexts.snapshotUpdate, cache)

        let create codec initial fold (isCurrentSnapshot, generate) streamId categoryName config =
            let cat = config |> function
                | Config.Cosmos (context, cache) -> createCategory categoryName codec initial fold isCurrentSnapshot (context, cache)
            Snapshotter.createService streamId generate cat
