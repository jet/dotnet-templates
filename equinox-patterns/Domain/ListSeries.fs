/// Maintains a pointer into the Epoch chain for a given Series
/// Allows the Ingester to determine the current Epoch into which it commence writing via ReadIngestionEpochId
/// As an Epoch is marked `Closed`, the Ingester will mark a new Epoch `Started` on this aggregate via MarkIngestionEpochId
module Patterns.Domain.ListSeries

let [<Literal>] Category = "ListSeries"
// TOCONSIDER: if you need multiple lists series/epochs in a single system, the Series and Epoch streams should have a SeriesId in the stream name
// See also the implementation in the feedSource template, where the Series aggregate also functions as an index of series held in the system
let streamName () = ListSeriesId.wellKnownId |> ListSeriesId.toString |> FsCodec.StreamName.create Category

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Started of            {| epochId : ListEpochId |}
        | Snapshotted of        {| active : ListEpochId |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = ListEpochId option
    let initial = None
    let private evolve _state = function
        | Events.Started e -> Some e.epochId
        | Events.Snapshotted e -> Some e.active
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot s = Events.Snapshotted {| active = Option.get s |}

let interpret epochId (state : Fold.State) =
    [if state |> Option.forall (fun cur -> cur < epochId) && epochId >= ListEpochId.initial then
        yield Events.Started {| epochId = epochId |}]

type Service internal (resolve_ : Equinox.ResolveOption option -> unit -> Equinox.Decider<Events.Event, Fold.State>) =

    let resolve = resolve_ None
    let resolveStale = resolve_ (Some Equinox.AllowStale)

    /// Determines the current active epoch
    /// Uses cached values as epoch transitions are rare, and caller needs to deal with the inherent race condition in any case
    member _.ReadIngestionEpochId() : Async<ListEpochId> =
        let decider = resolve ()
        decider.Query(Option.defaultValue ListEpochId.initial)

    /// Mark specified `epochId` as live for the purposes of ingesting
    /// Writers are expected to react to having writes to an epoch denied (due to it being Closed) by anointing a successor via this
    member _.MarkIngestionEpochId epochId : Async<unit> =
        let decider = resolveStale ()
        decider.Transact(interpret epochId)

let private create resolveStream =
    let resolve opt = streamName >> resolveStream opt >> Equinox.createDecider
    Service(resolve)

module MemoryStore =

    let create store =
        let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create resolveStream

module Cosmos =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create resolveStream
