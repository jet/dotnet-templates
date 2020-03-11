module Allocation

let [<Literal>] Category = "Allocation"
let streamName allocationId = FsCodec.StreamName.create Category (AllocationId.toString allocationId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] CategoryId = "Allocation"
    let (|For|) id = FsCodec.StreamName.create CategoryId (AllocationId.toString id)

    type Commenced =    { ticketIds : TicketId[] }
    type Tickets =      { ticketIds : TicketId[] }
    type Allocated =    { ticketIds : TicketId[]; listId : TicketListId }
    type Assigned =     { listId    : TicketListId }
    type Snapshotted =  { ticketIds : TicketId[] }
    type Event =
        /// Records full set of targets (so Abort can Revoke all potential in flight Reservations)
        | Commenced     of Commenced
        /// Tickets verified as not being attainable (Allocated, not just Reserved)
        | Failed        of Tickets
        /// Tickets verified as having been marked Reserved
        | Reserved      of Tickets
        /// Confirming cited tickets are to be allocated to the cited list
        | Allocated     of Allocated
        /// Records intention to release cited tickets (while Running, not implicitly via Aborted)
        | Released      of Tickets
        /// Transitioning to phase where (Commenced-Allocated) get Returned by performing Releases on the Tickets
        | Cancelled
        /// Confirming cited tickets have been assigned to the list
        | Assigned      of Assigned
        /// Records confirmed Revokes of cited Tickets
        | Revoked       of Tickets
        /// Allocated + Returned = Commenced ==> Open for a new Commenced to happen
        | Completed
        // Dummy event to make Equinox.EventStore happy (see `module EventStore`)
        | Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = NotStarted | Running of States | Canceling of States | Completed
    and States =
        {   unknown     : Set<TicketId>
            failed      : Set<TicketId>
            reserved    : Set<TicketId>
            assigning   : Events.Allocated list
            releasing   : Set<TicketId>
            stats       : Stats }
    and Stats =
        {   requested   : int
            denied      : int
            reserved    : int
            releasing   : int
            assigned    : int list }
    let (|Idle|Acquiring|Releasing|) = function NotStarted | Completed -> Idle | Running s -> Acquiring s | Canceling s -> Releasing s
    module States =
        let (|ToSet|) = set
        let private withKnown xs x =    { x with unknown = Set.difference x.unknown xs }
        let withFailed (ToSet xs) x =   { withKnown xs x with failed = x.failed |> Set.union xs }
        let withReserved (ToSet xs) x = { withKnown xs x with reserved = x.reserved |> Set.union xs }
        let withRevoked (ToSet xs) x =  { withKnown xs x with reserved = Set.difference x.reserved xs }
        let withReleasing (ToSet xs) x ={ withKnown xs x with releasing = x.releasing |> Set.union xs } // TODO
        let withAssigned listId x = // TODO
            let decided, remaining = x.assigning |> List.partition (fun x -> x.listId = listId)
            let xs = seq { for x in decided do yield! x.ticketIds }
            { withRevoked xs x with assigning = remaining }
    let initial = NotStarted
    let evolve state = function
        | Events.Commenced e ->
            match state with
            | NotStarted ->     Running { unknown = set e.ticketIds; failed = Set.empty; reserved = Set.empty; assigning = []; releasing = Set.empty
                                          stats = { requested = 0; denied = 0; reserved = 0; releasing = 0; assigned = [] } }
            | x ->              failwithf "Can only Commence when NotStarted, not %A" x
        | Events.Failed e ->
            match state with
            | Idle ->           failwith "Cannot have Failed if Idle"
            | Acquiring s ->    Running (s |> States.withFailed e.ticketIds)
            | Releasing s ->    Canceling (s |> States.withFailed e.ticketIds)
        | Events.Reserved e ->
            match state with
            | Idle ->           failwith "Cannot have Reserved if Idle"
            | Acquiring s ->    Running (s |> States.withReserved e.ticketIds)
            | Releasing s ->    Canceling (s |> States.withReserved e.ticketIds)
        | Events.Allocated e ->
            match state with
            | Idle ->           failwith "Cannot have Allocating if Idle"
            | Acquiring s ->    Running { s with assigning = e :: s.assigning}
            | Releasing s ->    Canceling { s with assigning = e :: s.assigning}
        | Events.Released e ->
            match state with
            | Idle ->           failwith "Cannot have Releasing if Idle"
            | Acquiring s ->    Running (s |> States.withReleasing e.ticketIds)
            | Releasing s ->    Canceling (s |> States.withReleasing e.ticketIds)
        | Events.Cancelled ->
            match state with
            | Acquiring s ->    Canceling s
            | x ->              failwithf "Can only Abort when Running, not %A" x
        | Events.Assigned e ->
            match state with
            | Idle ->           failwith "Cannot have Allocated if Idle"
            | Acquiring s ->    Running (s |> States.withAssigned e.listId)
            | Releasing s ->    Canceling (s |> States.withAssigned e.listId)
        | Events.Revoked e ->
            match state with
            | Idle ->           failwith "Cannot have Released if Idle"
            | Acquiring s ->    Running (s |> States.withRevoked e.ticketIds)
            | Releasing s ->    Canceling (s |> States.withRevoked e.ticketIds)
        | Events.Completed ->
            match state with
            | Acquiring s
            | Releasing s when Set.isEmpty s.unknown && Set.isEmpty s.reserved && List.isEmpty s.assigning ->
                                Completed
            | x ->              failwithf "Can only Complete when reservations and unknowns resolved, not %A" x
        | Events.Snapshotted -> state // Dummy event, see EventStore bindings
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Completed -> true | Events.Snapshotted | _ -> false

/// Current state of the workflow based on the present state of the Aggregate
type ProcessState =
    | NotStarted
    | Running       of reserved : TicketId list * toAssign : Events.Allocated list * toRelease : TicketId list * toReserve : TicketId list
    | Idle          of reserved : TicketId list
    | Cancelling    of                            toAssign : Events.Allocated list * toRelease : TicketId list
    | Completed
    static member FromFoldState = function
        | Fold.NotStarted ->
            NotStarted
        | Fold.Running e ->
            match Set.toList e.reserved, e.assigning, Set.toList e.releasing, Set.toList e.unknown with
            | res, [], [], [] ->
                Idle (reserved = res)
            | res, ass, rel, tor ->
                Running (reserved = res, toAssign = ass, toRelease = rel, toReserve = tor)
        | Fold.Canceling e ->
            Cancelling (toAssign = e.assigning, toRelease = [yield! e.reserved; yield! e.unknown; yield! e.releasing])
        | Fold.Completed ->
            Completed

/// Updates recording attained progress
type Update =
    | Failed        of tickets : TicketId list
    | Reserved      of tickets : TicketId list
    | Assigned      of listId  : TicketListId
    | Revoked       of tickets : TicketId list

let (|ToSet|) xs = set xs
let (|SetEmpty|_|) s = if Set.isEmpty s then Some () else None

/// Map processed work to associated events that are to be recorded in the stream
let decideUpdate update state =
    let owned (s : Fold.States) = Set.union s.releasing (set <| seq { yield! s.unknown; yield! s.reserved })
    match state, update with
    | (Fold.Completed | Fold.NotStarted), (Failed _|Reserved _|Assigned _|Revoked _) as x ->
        failwithf "Folds.Completed or NotStarted cannot handle (Failed|Revoked|Assigned) %A" x
    | (Fold.Running s|Fold.Canceling s), Reserved (ToSet xs) ->
        match set s.unknown |> Set.intersect xs with SetEmpty -> [] | changed -> [Events.Reserved { ticketIds = Set.toArray changed }]
    | (Fold.Running s|Fold.Canceling s), Failed (ToSet xs) ->
        match owned s |> Set.intersect xs with SetEmpty -> [] | changed -> [Events.Failed { ticketIds = Set.toArray changed }]
    | (Fold.Running s|Fold.Canceling s), Revoked (ToSet xs) ->
        match owned s |> Set.intersect xs with SetEmpty -> [] | changed -> [Events.Revoked { ticketIds = Set.toArray changed }]
    | (Fold.Running s|Fold.Canceling s), Assigned listId ->
        if s.assigning |> List.exists (fun x -> x.listId = listId) then [Events.Assigned { listId = listId }] else []

/// Holds events accumulated from a series of decisions while also evolving the presented `state` to reflect the pended events
type private Accumulator() =
    let acc = ResizeArray()
    member __.Ingest state : 'res * Events.Event list -> 'res * Fold.State = function
        | res, [] ->                   res, state
        | res, [e] -> acc.Add e;       res, Fold.evolve state e
        | res, xs ->  acc.AddRange xs; res, Fold.fold state (Seq.ofList xs)
    member __.Accumulated = List.ofSeq acc

/// Impetus provided to the Aggregate Service from the Process Manager
type Command =
    | Commence      of tickets : TicketId list
    | Apply         of assign  : Events.Allocated list * release : TicketId list
    | Cancel

/// Apply updates, decide whether Command is applicable, emit state reflecting work to be completed to conclude the in-progress workflow (if any)
let sync (updates : Update seq, command : Command) (state : Fold.State) : (bool*ProcessState) * Events.Event list =
    let acc = Accumulator()

    (* Apply any updates *)
    let mutable state = state
    for x in updates do
        let (), state' = acc.Ingest state ((), decideUpdate x state)
        state <- state'

    (* Decide whether the Command is now acceptable *)
    let accepted, state =
        acc.Ingest state <|
            match state, command with
            (* Ignore on the basis of being idempotent in the face of retries *)
            // TOCONSIDER how to represent that a request is being denied e.g. due to timeout vs due to being complete
            | (Fold.Idle|Fold.Releasing _), Apply _ ->
                false, []
            (* Defer; Need to allow current request to progress before it can be considered *)
            | (Fold.Acquiring _|Fold.Releasing _), Commence _ ->
                true, [] // TODO validate idempotent ?
            (* Ok on the basis of idempotency *)
            | (Fold.Idle|Fold.Releasing _), Cancel ->
                true, []
            (* Ok; Currently idle, normal Commence request*)
            | Fold.Idle, Commence tickets ->
                true, [Events.Commenced { ticketIds = Array.ofList tickets }]
            (* Ok; normal apply to distribute held tickets *)
            | Fold.Acquiring s, Apply (assign, release) ->
                let avail = System.Collections.Generic.HashSet s.reserved
                let toAssign = [for a in assign -> { a with ticketIds = a.ticketIds |> Array.where avail.Remove }]
                let toRelease = (Set.empty, release) ||> List.fold (fun s x -> if avail.Remove x then Set.add x s else s)
                true, [
                    for x in toAssign do if (not << Array.isEmpty) x.ticketIds then yield Events.Allocated x
                    match toRelease with SetEmpty -> () | toRelease -> yield Events.Released { ticketIds = Set.toArray toRelease }]
            (* Ok, normal Cancel *)
            | Fold.Acquiring _, Cancel ->
                true, [Events.Cancelled]

    (* Yield outstanding processing requirements (if any), together with events accumulated based on the `updates` *)
    (accepted, ProcessState.FromFoldState state), acc.Accumulated

type Service internal (resolve : AllocationId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Sync(allocationId, updates, command) : Async<bool*ProcessState> =
        let stream = resolve allocationId
        stream.Transact(sync (updates, command))

let create resolve =
    let resolve pickListId =
        let stream = resolve (streamName pickListId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 2)
    Service(resolve)

module EventStore =

    // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
    let opt = Equinox.ResolveOption.AllowStale
    let create (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // We should be reaching Completed state frequently so no actual Snapshots should get written
        let resolver = Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy)
        let resolve id = resolver.Resolve(id, opt)
        create resolve

module Cosmos =

    // TODO impl snapshots
    let makeEmptyUnfolds events _state = events, []
    let accessStrategy = Equinox.Cosmos.AccessStrategy.Custom (Fold.isOrigin,makeEmptyUnfolds)
    // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
    let opt = Equinox.ResolveOption.AllowStale
    let create (context, cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolve id = resolver.Resolve(id,opt)
        create resolve