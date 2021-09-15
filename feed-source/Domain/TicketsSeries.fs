/// Maintains a pointer for into the TicketsEpoch chain for each FC
/// Allows an Ingester to quickly determine the current Epoch which it should commence writing into
/// As an Epoch is marked `Closed`, `module Tickets` will mark a new epoch `Started` on this aggregate
/// Can also be used to walk back through time to visit every ticket there has ever been for correlation purposes
module FeedSourceTemplate.Domain.TicketsSeries

let [<Literal>] Category = "Tickets"
let streamName seriesId = FsCodec.StreamName.create Category (TicketsSeriesId.toString seriesId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Started of {| fcId : FcId; epochId : TicketsEpochId |}
        | Snapshotted of {| active : Map<FcId, TicketsEpochId> |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Map<FcId, TicketsEpochId>

    let initial = Map.empty
    let evolve state = function
        | Events.Started e -> state |> Map.add e.fcId e.epochId
        | Events.Snapshotted e -> e.active
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = s |}

let readEpochId fcId (state : Fold.State) =
    state
    |> Map.tryFind fcId

let interpret (fcId, epochId) (state : Fold.State) =
    [if state |> readEpochId fcId |> Option.forall (fun cur -> cur < epochId) && epochId >= TicketsEpochId.initial then
        yield Events.Started {| fcId = fcId; epochId = epochId |}]

type EpochDto = { fc : FcId; ingestionEpochId : TicketsEpochId }
module EpochDto =
    let ofState (s : Fold.State) = seq {
        for x in s -> { fc = x.Key; ingestionEpochId = x.Value }
    }

type Service internal (resolve : TicketsSeriesId -> Equinox.Decider<Events.Event, Fold.State>, ?seriesId) =

    // For now we have a single global sequence. This provides us an extension point should we ever need to reprocess
    // NOTE we use a custom id in order to isolate data for acceptance tests
    let seriesId = defaultArg seriesId TicketsSeriesId.wellKnownId

    /// Exposes the set of tranches for which data is held, enabling a consumer to crawl the full dataset
    member _.ReadIngestionEpochs() : Async<EpochDto seq> =
        let decider = resolve seriesId
        decider.Query EpochDto.ofState

    /// Determines the current active epoch for the specified `fcid`
    member _.TryReadIngestionEpochId fcid : Async<TicketsEpochId option> =
        let decider = resolve seriesId
        decider.Query(readEpochId fcid)

    /// Mark specified `epochId` as live for the purposes of ingesting TicketIds
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing the successor via this
    member _.MarkIngestionEpochId(fcid, epochId) : Async<unit> =
        let decider = resolve seriesId
        decider.Transact(interpret (fcid, epochId))

module Config =

    let private create seriesOverride resolveStream =
        let resolve = streamName >> resolveStream Equinox.AllowStale >> Equinox.createDecider
        Service(resolve, ?seriesId=seriesOverride)

    module Memory =

        let create store =
            let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
            let resolveStream opt sn = cat.Resolve(sn, opt)
            create None resolveStream

    module Cosmos =

        open Equinox.CosmosStore

        let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
        let  create (context, cache) =
            let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
            let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
            let resolveStream opt sn = cat.Resolve(sn, opt)
            create None resolveStream
