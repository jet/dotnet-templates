module Allocator

open System

let [<Literal>] Category = "Allocator"
let streamName allocatorId = FsCodec.StreamName.create Category (AllocatorId.toString allocatorId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Commenced =    { allocationId : AllocationId; cutoff : DateTimeOffset }
    type Completed =    { allocationId : AllocationId; reason : Reason }
    and  [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.TypeSafeEnumConverter>)>]
         Reason = Ok | TimedOut | Cancelled
    type Snapshotted =  { active : Commenced option }
    type Event =
        | Commenced     of Commenced
        | Completed     of Completed
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Events.Commenced option
    let initial = None
    let evolve _state = function
        | Events.Commenced e -> Some e
        | Events.Completed _ -> None
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

type CommenceResult = Accepted | Conflict of AllocationId

let decideCommence allocationId cutoff : Fold.State -> CommenceResult*Events.Event list = function
    | None -> Accepted, [Events.Commenced { allocationId = allocationId; cutoff = cutoff }]
    | Some { allocationId = tid } when allocationId = tid -> Accepted, [] // Accept replay idempotently
    | Some curr -> Conflict curr.allocationId, [] // Reject attempts at commencing overlapping transactions

let decideComplete allocationId reason : Fold.State -> Events.Event list = function
    | Some { allocationId = tid } when allocationId = tid -> [Events.Completed { allocationId = allocationId; reason = reason }]
    | Some _ | None -> [] // Assume replay; accept but don't write

type Service internal (resolve : AllocatorId  -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Commence(allocatorId, allocationId, cutoff) : Async<CommenceResult> =
        let stream = resolve allocatorId
        stream.Transact(decideCommence allocationId cutoff)

    member __.Complete(allocatorId, allocationId, reason) : Async<unit> =
        let stream = resolve allocatorId
        stream.Transact(decideComplete allocationId reason)

let create resolver =
    let resolve pickListId =
        let stream = resolver (streamName pickListId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service (resolve)

module EventStore =

    let accessStrategy = Equinox.EventStore.AccessStrategy.LatestKnownEvent
    let resolver (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve(id,opt)
    let create (context, cache) =
        create (resolver (context, cache))

module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.LatestKnownEvent
    let resolver (context,cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve(id,opt)
    let create (context, cache) =
        create (resolver (context, cache))
