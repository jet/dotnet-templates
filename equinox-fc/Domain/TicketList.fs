module TicketList

let [<Literal>] Category = "TicketList"
let streamName listId = FsCodec.StreamName.create Category (TicketListId.toString listId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] CategoryId = "TicketList"
    let (|For|) id = FsCodec.StreamName.create CategoryId (TicketListId.toString id)

    type Allocated =    { allocatorId : AllocatorId; ticketIds : TicketId[] }
    type Snapshotted =  { ticketIds : TicketId[] }
    type Event =
        | Allocated     of Allocated
        | Snapshotted   of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Set<TicketId>
    let initial = Set.empty
    let evolve state = function
        | Events.Allocated e -> (state, e.ticketIds) ||> Array.fold (fun m x -> Set.add x m)
        | Events.Snapshotted e -> Set.ofArray e.ticketIds
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | Events.Allocated _ -> false
    let snapshot state = Events.Snapshotted { ticketIds = Set.toArray state }

let interpret (allocatorId : AllocatorId, allocated : TicketId list) (state : Fold.State) : Events.Event list =
    match allocated |> Seq.except state |> Seq.distinct |> Seq.toArray with
    | [||] -> []
    | news -> [Events.Allocated { allocatorId = allocatorId; ticketIds = news }]

type Service internal (resolve : TicketListId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Sync(pickListId, allocatorId, assignedTickets) : Async<unit> =
        let stream = resolve pickListId
        stream.Transact(interpret (allocatorId, assignedTickets))

let create resolve =
    let resolve pickListId =
        let stream = resolve (streamName pickListId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service(resolve)

module EventStore =

    // we _could_ use this Access Strategy, but because we are only generally doing a single shot write, its unwarranted
    // let accessStrategy = AccessStrategy.RollingSnapshots (Folds.isOrigin, Folds.snapshot)
    // while there are competing writers (which might cause us to have to retry a Transact and discover it is redundant), there is never a cost to being wrong
    let opt = Equinox.ResolveOption.AllowStale
    let create (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy)
        let resolve id = resolver.Resolve(id,opt)
        create resolve

module Cosmos =

    // while there are competing writers (which might cause us to have to retry a Transact and discover it is redundant), there is never a cost to being wrong
    let opt = Equinox.ResolveOption.AllowStale
    // we want reads and writes (esp idempotent ones) to have optimal RU efficiency so we go the extra mile to do snapshotting into the Tip
    let accessStrategy = Equinox.Cosmos.AccessStrategy.Snapshot (Fold.isOrigin, Fold.snapshot)
    let create (context, cache)=
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolve id = resolver.Resolve(id, opt)
        create resolve