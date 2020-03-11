module Allocator

open System

let [<Literal>] Category = "Allocator"
let streamName allocatorId = FsCodec.StreamName.create Category (AllocatorId.toString allocatorId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] CategoryId = "Allocator"
    let (|For|) id = FsCodec.StreamName.create CategoryId (AllocatorId.toString id)

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

let create resolve =
    let resolve pickListId =
        let stream = resolve (streamName pickListId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service(resolve)

module EventStore =

    let accessStrategy = Equinox.EventStore.AccessStrategy.LatestKnownEvent
    let create (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        let resolver = Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolve id = resolver.Resolve(id,opt)
        create resolve

module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.LatestKnownEvent
    // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
    let opt = Equinox.ResolveOption.AllowStale
    let create (context, cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolve id = resolver.Resolve(id,opt)
        create resolve
