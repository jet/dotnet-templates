/// Tracks all Tickets that entered the system over a period of time
/// - Used to walk back through the history of all tickets in the system in approximate order of their processing
/// - Limited to a certain reasonable count of items; snapshot of Tickets in an epoch needs to stay a sensible size
/// The TicketsSeries holds a pointer to the current active epoch for each FC
/// Each successive epoch is identified by an index, i.e. TicketsEpoch-FC001_0, then TicketsEpoch-FC001_1
module FeedApiTemplate.Domain.TicketsEpoch

let [<Literal>] Category = "TicketsEpoch"
let streamName (fcId, epochId) = FsCodec.StreamName.compose Category [FcId.toString fcId; TicketsEpochId.toString epochId]

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Ingested    of {| ids : TicketId[] |}
        | Closed
        | Snapshotted of {| ids : TicketId[]; closed : bool |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = TicketId[] * bool
    let initial = [||], false
    let evolve (ids, closed) = function
        | Events.Ingested e     -> Array.append e.ids ids, closed
        | Events.Snapshotted e  -> e.ids, e.closed
        | Events.Closed         -> ids, true

    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot (ids, closed) = Events.Snapshotted {| ids = ids; closed = closed |}

type Result = { rejected : TicketId[]; added : TicketId[]; isClosed : bool; content : TicketId[] }

let decide capacity candidateIds = function
    | currentIds, false as state ->
        let added, events =
            match candidateIds |> Array.except currentIds with
            | [||] -> [||], []
            | news ->
                let closing = Array.length currentIds + Array.length news >= capacity
                let ingestEvent = Events.Ingested {| ids = news |}
                news, if closing then [ ingestEvent ; Events.Closed ] else [ ingestEvent ]
        let state' = Fold.fold state events
        { rejected = [||]; added = added; isClosed = snd state'; content = fst state' }, events
    | currentIds, true ->
        { rejected = candidateIds |> Array.except currentIds; added = [||]; isClosed = true; content = currentIds }, []

type StateDto = { closed : bool; tickets : TicketId[] }

/// Service used for the write side; manages ingestion of items into the series of epochs
type IngestionService internal (capacity, resolve : FcId * TicketsEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Handles idempotent deduplicated insertion into the set of items held within the epoch
    member _.Ingest(fcid, epochId, ticketIds) : Async<Result> =
        let decider = resolve (fcid, epochId)
        decider.Transact(decide capacity ticketIds)

    /// Obtains a complete list of all the tickets in the specified fcid/epochId
    member _.ReadTickets(fcid, epochId) : Async<TicketId[]> =
        let decider = resolve (fcid, epochId)
        decider.Query fst

    static member internal Create(capacity, resolveStream) =
        // Accept whatever date is in the cache on the basis that we are doing most of the writing so will more often than not
        // have the correct state already without a roundtrip. What if the data is actually stale? we\ll end up needing to resync,
        // but we we need to deal with that as a race condition anyway
        let resolve = streamName >> resolveStream (Some Equinox.AllowStale) >> Equinox.createDecider
        IngestionService(capacity, resolve)

/// Handles the rendering of items as a feed, covering the read side
type ReadService internal (resolve : FcId * TicketsEpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Yields the current state of this epoch, including an indication of whether reading has completed
    member _.Read(fcid, epochId) : Async<StateDto> =
        let decider = resolve (fcid, epochId)
        decider.Query(fun (tickets, closed) -> { closed = closed; tickets = tickets })

    static member internal Create(resolveStream) =
        let resolve = streamName >> resolveStream None >> Equinox.createDecider
        ReadService(resolve)

module Cosmos =

    open Equinox.CosmosStore

    let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin, Fold.toSnapshot)
    let private resolveStream (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        fun opt sn -> cat.Resolve(sn, ?option = opt)

    let createIngester capacity (context, cache) = IngestionService.Create(capacity, resolveStream (context, cache))
    let createReader (context, cache) = ReadService.Create(resolveStream (context, cache))
