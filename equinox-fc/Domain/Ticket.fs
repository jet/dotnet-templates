module Ticket

let [<Literal>] Category = "Ticket"
let streamName ticketId = FsCodec.StreamName.create Category (TicketId.toString ticketId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] CategoryId = "Ticket"
    let (|For|) id = FsCodec.StreamName.create CategoryId (TicketId.toString id)

    type Reserved =     { allocatorId : AllocatorId }
    type Allocated =    { allocatorId : AllocatorId; listId : TicketListId }

    type Event =
        | Reserved      of Reserved
        | Allocated     of Allocated
        | Revoked
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Unallocated | Reserved of by : AllocatorId | Allocated of by : AllocatorId * on : TicketListId
    let initial = Unallocated
    let private evolve _state = function
        | Events.Reserved e -> Reserved e.allocatorId
        | Events.Allocated e -> Allocated (e.allocatorId, e.listId)
        | Events.Revoked -> Unallocated
    // because each event supersedes the previous one, we only ever need to fold the last event
    let fold state events =
        Seq.tryLast events |> Option.fold evolve state

type Command =
    /// permitted if nobody owns it (or idempotently ok if we are the owner)
    | Reserve
    /// permitted if the allocator has it reserved (or idempotently ok if already on list)
    | Allocate of on : TicketListId
    /// must be performed by the owner; attempts by non-owner to deallocate get ignored as a new owner now has that responsibility
    /// (but are not failures from an Allocator's perspective)
    | Revoke

let decide (allocator : AllocatorId) (command : Command) (state : Fold.State) : bool * Events.Event list =
    match command, state with
    | Reserve, Fold.Unallocated -> true, [Events.Reserved { allocatorId = allocator }] // normal case -> allow+record
    | Reserve, Fold.Reserved by when by = allocator -> true, [] // idempotently permit
    | Reserve, (Fold.Reserved _ | Fold.Allocated _) -> false, [] // report failure, nothing to write
    | Allocate list, Fold.Allocated (by, l) when by = allocator && l = list -> true, [] // idempotent processing
    | Allocate list, Fold.Reserved by when by = allocator -> true, [Events.Allocated { allocatorId = allocator; listId = list }] // normal
    | Allocate _, (Fold.Allocated _ | Fold.Unallocated | Fold.Reserved _) -> false, [] // Fail if someone else has reserved or allocated, or we are jumping straight to Allocated without Reserving first
    | Revoke, Fold.Unallocated -> true, [] // idempotent handling
    | Revoke, (Fold.Reserved by | Fold.Allocated (by, _)) when by = allocator -> true, [Events.Revoked] // release Reservation or Allocation
    | Revoke, (Fold.Reserved _ | Fold.Allocated _ ) -> true, [] // NOTE we report success of achieving the intent (but, critically, we leave it to the actual owner to manage any actual revoke)

type Service internal (resolve : TicketId -> Equinox.Stream<Events.Event, Fold.State>) =

    /// Attempts to achieve the intent represented by `command`. High level semantics as per comments on Command (see decide for lowdown)
    /// `false` is returned if a competing allocator holds it (or we're attempting to jump straight to Allocated without first Reserving)
    member __.Sync(pickTicketId, allocator, command : Command) : Async<bool> =
        let stream = resolve pickTicketId
        stream.Transact(decide allocator command)

let create resolve =
    let resolve ticketId =
        let stream = resolve (streamName ticketId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service(resolve)

module EventStore =

    let accessStrategy = Equinox.EventStore.AccessStrategy.LatestKnownEvent
    let create (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // because we only ever need the last event, we use the Equinox.EventStore access strategy that optimizes around that
        let resolver = Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve

module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.LatestKnownEvent
    let create (context, cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // because we only ever need the last event to build the state, we feed the events we are writing
        // (there's always exactly one if we are writing), into the unfolds slot so a single point read with etag check gets us state in one trip
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
