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

    type Result =
        | Valid // No-op case: no update required as the stream already has a correct snapshot
        | Invalid // Update skipped due to running in dryRun mode; we avoided running the update
        | Updated // Update required: yield a tentative event (which transmuteAllEventsToUnfolds will flip to being an unfold)
    let decide generate dryRun (hasSnapshot, state) =
        if hasSnapshot then Valid, Array.empty
        elif dryRun then Invalid, Array.empty
        // Note Updated is a synthetic/tentative event, which transmuteAllEventsToUnfolds will use as a signal to a) update the unfolds b) drop the event
        else Updated, generate state
    type private StateWithSnapshottedFlag<'s> = bool * 's
    type Service<'id, 'e, 's> internal (resolve: 'id -> Equinox.Decider<'e, StateWithSnapshottedFlag<'s>>, generate: 's -> 'e[]) =
        member _.TryUpdate(id, dryRun): Async<Result * int64> =
            let decider = resolve id
            decider.TransactWithPostVersion(decide generate dryRun)
    module Service =
        let tryUpdate dryRun (x: Service<_, _, _>) id = x.TryUpdate(id, dryRun)
    let internal createService streamId generate cat =
        let resolve = streamId >> createDecider cat
        Service(resolve, generate)

    let internal initial'<'s> initial: StateWithSnapshottedFlag<'s> = false, initial
    let internal fold' isValidUnfolds fold (_wasOrigin, s) xs: StateWithSnapshottedFlag<'s> =
        // NOTE ITimelineEvent.IsUnfold and/or a generic isOrigin event would be insufficient for our needs
        // The tail event encountered by the fold could either be:
        // - an 'out of date' snapshot (which the normal load process would be able to upconvert from, but is not what we desire)
        // - another event (if there is no snapshot of any kind)
        isValidUnfolds xs, fold s xs

module Ingester =

    open FsCodec
    type internal Event<'e, 'f> = (struct (ITimelineEvent<'f> * 'e))
    let internal createCodec<'e, 'f, 'c> (target: IEventCodec<'e, 'f, 'c>): IEventCodec<Event<'e, 'f>, 'f, 'c>  =
        let encode (c: 'c) ((input, upped): Event<'e, 'f>) : struct (string * 'f * 'f * System.Guid * string * string * System.DateTimeOffset) =
            let e = target.Encode(c, upped)
            e.EventType, e.Data, input.Meta, input.EventId, input.CorrelationId, input.CausationId, input.Timestamp
        let decode (e: ITimelineEvent<'f>): Event<'e, 'f> voption = match target.Decode e with ValueNone -> ValueNone | ValueSome d -> ValueSome (e, d)
        Codec.Create<Event<'e, 'f>, 'f, 'c>(encode, decode)

    type private State = unit
    let internal initial: State = ()
    let internal fold () = ignore

    let private decide (inputCodec: IEventCodec<'e, 'f, unit>) (inputs: ITimelineEvent<'f>[]) (c: Equinox.ISyncContext<unit>): Event<'e, 'f>[] = [|
        for x in inputs do
            if x.Index >= c.Version then // NOTE source and target need to have 1:1 matching event indexes, or things would be much more complex
                match inputCodec.Decode x with
                | ValueNone -> failwith $"Unknown EventType {x.EventType} at index {x.Index}"
                | ValueSome d -> struct (x, d) |] // So we require all source events to exactly one event in the target

    type Service<'id, 'e, 's, 'f> internal (codec: IEventCodec<'e, 'f, unit>, resolve: 'id -> Equinox.Decider<Event<'e, 'f>, State>) =
        member _.Ingest(id, sourceEvents: ITimelineEvent<'f>[]): Async<int64> =
            let decider = resolve id
            decider.TransactEx(decide codec sourceEvents, fun (x: Equinox.ISyncContext<State>) -> x.Version)
    let internal createService<'id, 'e, 'f> streamId inputCodec cat =
        let resolve = streamId >> createDecider cat
        Service<'id, 'e, unit, 'f>(inputCodec, resolve)
    module Service =
        let ingest (svc: Service<'id, 'e, 's, System.Text.Json.JsonElement>) id (events: ITimelineEvent<Encoded>[]) =
            let events = events |> Array.map (FsCodec.Core.TimelineEvent.mapBodies FsCodec.SystemTextJson.Encoding.Utf8EncodedToJsonElement)
            svc.Ingest(id, events)

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
    
    let eventEncoding codec shouldCompress =
        match shouldCompress with
        | None -> FsCodec.SystemTextJson.Encoder.Uncompressed codec
        | Some predicate -> FsCodec.SystemTextJson.Encoder.Compressed(codec, shouldCompress = fun (x: FsCodec.IEventData<System.Text.Json.JsonElement>) -> predicate x.EventType)
    let private createCached name codec initial fold accessStrategy shouldCompress (context, cache) =
        CosmosStoreCategory(context, name,eventEncoding codec shouldCompress, fold, initial, accessStrategy, cacheStrategy cache)

    let createSnapshotted name codec initial fold (isOrigin, toSnapshot, shouldCompress) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        createCached name codec initial fold accessStrategy (Some shouldCompress) (context.main, cache)

    let createRollingState name codec initial fold toSnapshot (context, cache) =
        let accessStrategy = AccessStrategy.RollingState toSnapshot
        createCached name codec initial fold accessStrategy None (context.views, cache)

    let createConfig (main, views, snapshotUpdate) cache =
        Config.Cosmos ({ main = main; views = views; snapshotUpdate = snapshotUpdate }, cache)

    module Snapshotter =

        let private accessStrategy isOrigin =
            let transmuteAllEventsToUnfolds events _state = [||], events
            AccessStrategy.Custom (isOrigin, transmuteAllEventsToUnfolds)
        let private createCategory name codec initial fold isValidUnfolds accessStrategy shouldCompress (contexts, cache) =
            createCached name codec (Snapshotter.initial' initial) (Snapshotter.fold' isValidUnfolds fold) accessStrategy shouldCompress (contexts.snapshotUpdate, cache)
        /// Equinox allows any number of unfold events to be stored:
        /// - the `isOrigin` predicate identifies the "main snapshot" - if it returns `true`, we don't need to load and fold based on events
        /// - `isValidUnfolds` inspects the full set of unfolds in order to determine whether they are complete
        ///   - where Index Unfolds are required for application functionality, we can trigger regeneration where they are missing
        let withIndexing codec initial fold (isOrigin, isValidUnfolds, generateUnfolds, shouldCompress) streamId categoryName config forceLoadingAllEvents =
            let accessStrategy = (if forceLoadingAllEvents then fun _ -> false else isOrigin) |> accessStrategy
            let cat = config |> function
                | Config.Cosmos (context, cache) -> createCategory categoryName codec initial fold isValidUnfolds accessStrategy shouldCompress (context, cache)
            Snapshotter.createService streamId generateUnfolds cat
        /// For the common case where we don't use any indexing - we only have a single relevant unfold to detect, and a function to generate it
        let single codec initial fold (isOrigin, generate, shouldCompress) streamId categoryName config =
            withIndexing codec initial fold (isOrigin, Array.tryLast >> Option.exists isOrigin, generate >> Array.singleton, Some shouldCompress) streamId categoryName config

    module Ingester =

        let private slice eventSize struct (maxEvents, maxBytes) span =
            let mutable countBudget, bytesBudget = maxEvents, maxBytes
            let withinLimits y =
                countBudget <- countBudget - 1
                bytesBudget <- bytesBudget - eventSize y
                // always send at least one event in order to surface the problem and have the stream marked malformed
                countBudget = maxEvents - 1 || (countBudget >= 0 && bytesBudget >= 0)
            span |> Array.takeWhile withinLimits
        // We gauge the likely output size from the input size
        // (to be 100% correct, we should encode it as the Sync in Equinox would do for the real converted data)
        // (or, to completely cover/gold plate it, we could have an opt-in on the Category to do slicing internally)
        let eventSize ((x, _e): Ingester.Event<_, _>) = x.Size
        let private accessStrategy =
            let isOriginIgnoreEvents _ = true // we only need to know the Version to manage the ingestion process
            let transmuteTrimsToStoredProcInputLimitAndDoesNotGenerateUnfolds events () =
                let maxEvents, maxBytes = 16384, 256 * 1024
                let trimmed = slice eventSize (maxEvents, maxBytes) events
                trimmed, Array.empty
            AccessStrategy.Custom (isOriginIgnoreEvents, transmuteTrimsToStoredProcInputLimitAndDoesNotGenerateUnfolds)
        let private createCategory name codec (context, cache) =
            createCached name codec Ingester.initial Ingester.fold accessStrategy None (context, cache)

        type TargetCodec<'e> = FsCodec.IEventCodec<'e, System.Text.Json.JsonElement, unit>
        let create<'id, 'e> struct (inputStreamCodec: FsCodec.IEventCodec<'e, FsCodec.Encoded, unit>, targetCodec: TargetCodec<'e>) streamId categoryName struct (context, cache) =
            let rewriteEventBodiesCodec = Ingester.createCodec<'e, System.Text.Json.JsonElement, unit> targetCodec
            let cat = createCategory categoryName rewriteEventBodiesCodec (context, cache)
            let inputStreamToJsonElement = inputStreamCodec |> FsCodec.SystemTextJson.Encoder.Utf8AsJsonElement
            Ingester.createService<'id, 'e, System.Text.Json.JsonElement> streamId inputStreamToJsonElement cat
