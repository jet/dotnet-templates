module Fc.Inventory.Series

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] CategoryId = "InventorySeries"
    let (|For|) inventoryId = FsCodec.StreamName.create CategoryId (InventoryId.toString inventoryId)

    type Epoch = { epoch : InventoryEpochId }
    type Checkpoint = { epoch : InventoryEpochId; index : int64 }
    type Snapshotted = { writing : InventoryEpochId; checkpoint : Checkpoint option }
    type Event =
        | Started of Epoch
        | Checkpointed of Checkpoint
        | Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = { writing : InventoryEpochId; checkpoint : Events.Checkpoint option }

    let initial = { writing = 0<inventoryEpochId>; checkpoint = None }
    let evolve state = function
        | Events.Started e -> { state with writing = e.epoch }
        | Events.Checkpointed e -> { state with checkpoint = Some e }
        | Events.Snapshotted e -> { writing = e.writing; checkpoint = e.checkpoint }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot s = Events.Snapshotted { writing = s.writing; checkpoint = s.checkpoint }
    // TODO transmute to remove checkpoints a la Propulsion

let interpretMarkWriting epochId (state : Fold.State) =
    if state.writing >= epochId then []
    else [Events.Started { epoch = epochId }]

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For streamId) = Equinox.Stream<Events.Event,Fold.State>(log, resolve streamId, maxAttempts)

    member __.ReadIngestionEpochId inventoryId : Async<InventoryEpochId> =
        let stream = resolve inventoryId
        stream.Query(fun s -> s.writing)

    member __.UpdateEpoch(inventoryId, epochId) : Async<unit> =
        let stream = resolve inventoryId
        stream.Transact(interpretMarkWriting epochId)

    // TODO checkpoint writing

let createService resolve =
    Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 2)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // For this stream, we uniformly use stale reads as:
        // a) we don't require any information from competing writers
        // b) while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.snapshot)
        fun id -> Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve(id,opt)
    let createService (context, cache) =
        createService (resolve (context, cache))
