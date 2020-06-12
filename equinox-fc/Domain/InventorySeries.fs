/// Manages a) the ingestion epoch id b) the current checkpointed read position for a long-running Inventory Series
/// See InventoryEpoch for the logic managing the actual events logged within a given epoch
/// See Inventory.Service for the surface API which manages the writing
module Fc.Domain.Inventory.Series

let [<Literal>] Category = "InventorySeries"
let streamName inventoryId = FsCodec.StreamName.create Category (InventoryId.toString inventoryId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Started = { epoch : InventoryEpochId }
    type Event =
        | Started of Started
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = InventoryEpochId option
    let initial = None
    let evolve _state = function
        | Events.Started e -> Some e.epoch
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

let queryActiveEpoch state = state |> Option.defaultValue (InventoryEpochId.parse 0)

let interpretAdvanceIngestionEpoch epochId (state : Fold.State) =
    if queryActiveEpoch state >= epochId then []
    else [Events.Started { epoch = epochId }]

type Service internal (resolve : InventoryId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.ReadIngestionEpoch(inventoryId) : Async<InventoryEpochId> =
        let stream = resolve inventoryId
        stream.Query queryActiveEpoch

    member __.AdvanceIngestionEpoch(inventoryId, epochId) : Async<unit> =
        let stream = resolve inventoryId
        stream.Transact(interpretAdvanceIngestionEpoch epochId)

let create resolve =
    // For this stream, we uniformly use stale reads as:
    // a) we don't require any information from competing writers
    // b) while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
    let opt = Equinox.ResolveOption.AllowStale
    let resolve locationId =
        let stream = resolve (streamName locationId, opt)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service(resolve)

module Cosmos =

    open Equinox.Cosmos

    let accessStrategy = AccessStrategy.LatestKnownEvent
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create <| fun (id, opt) -> resolver.Resolve(id, opt)

module EventStore =

    open Equinox.EventStore

    let accessStrategy = AccessStrategy.LatestKnownEvent
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create <| fun (id, opt) -> resolver.Resolve(id, opt)
