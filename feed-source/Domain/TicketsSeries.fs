/// Maintains a pointer for into the TicketsEpoch chain for each FC
/// Allows an Ingester to quickly determine the current Epoch which it should commence writing into
/// As an Epoch is marked `Closed`, `module Tickets` will mark a new epoch `Started` on this aggregate
/// Can also be used to walk back through time to visit every ticket there has ever been for correlation purposes
module FeedSourceTemplate.Domain.TicketsSeries

module private Stream =
    let [<Literal>] Category = "Tickets"
    let id = FsCodec.StreamId.gen TicketsSeriesId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Started of {| fcId: FcId; epochId: TicketsEpochId |}
        | Snapshotted of {| active: Map<FcId, TicketsEpochId> |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>

module Fold =

    type State = Map<FcId, TicketsEpochId>

    let initial = Map.empty
    let evolve state = function
        | Events.Started e -> state |> Map.add e.fcId e.epochId
        | Events.Snapshotted e -> e.active
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = s |}

let readEpochId fcId (state: Fold.State) =
    state
    |> Map.tryFind fcId

let interpret (fcId, epochId) (state: Fold.State) = [|
    if state |> readEpochId fcId |> Option.forall (fun cur -> cur < epochId) && epochId >= TicketsEpochId.initial then
        Events.Started {| fcId = fcId; epochId = epochId |} |]

type EpochDto = { fc: FcId; ingestionEpochId: TicketsEpochId }
module EpochDto =
    let ofState (s: Fold.State) = seq {
        for x in s -> { fc = x.Key; ingestionEpochId = x.Value }
    }

type Service internal (seriesId, resolve: TicketsSeriesId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Exposes the set of tranches for which data is held, enabling a consumer to crawl the full dataset
    member _.ReadIngestionEpochs(): Async<EpochDto seq> =
        let decider = resolve seriesId
        decider.Query EpochDto.ofState

    /// Determines the current active epoch for the specified `fcId`
    member _.TryReadIngestionEpochId fcId: Async<TicketsEpochId option> =
        let decider = resolve seriesId
        decider.Query(readEpochId fcId)

    /// Mark specified `epochId` as live for the purposes of ingesting TicketIds
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing the successor via this
    member _.MarkIngestionEpochId(fcId, epochId): Async<unit> =
        let decider = resolve seriesId
        decider.Transact(interpret (fcId, epochId))

module Factory =

    let private create_ seriesId resolve =
        // For now we have a single global sequence. This provides us an extension point should we ever need to reprocess
        // NOTE we use a custom id in order to isolate data for acceptance tests
        let seriesId = defaultArg seriesId TicketsSeriesId.wellKnownId
        Service(seriesId, Stream.id >> resolve)
    let private (|Category|) = function
        | Store.Context.Memory store ->            Store.Memory.create Stream.Category Events.codec Fold.initial Fold.fold store
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Stream.Category Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
    let create seriesOverride (Category cat) = create_ seriesOverride (Store.createDecider cat)
