module TodoBackendTemplate.Todo

let [<Literal>] Category = "Todos"
/// Maps a ClientId to the StreamId portion of the StreamName where data for that client will be held
let streamId = Equinox.StreamId.gen ClientId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData =     { id : int; order : int; title : string; completed : bool }
    type DeletedData =  { id : int }
    type ClearedData =  { nextId : int }
    type SnapshotData = { nextId : int; items : ItemData[] }
    /// Events we keep in Todo-* streams
    type Event =
        | Added         of ItemData
        | Updated       of ItemData
        | Deleted       of DeletedData
        /// Cleared also `isOrigin` (see below) - if we see one of these, we know we don't need to look back any further
        | Cleared       of ClearedData
        /// For Cosmos, AccessStrategy.Snapshot maintains this as an event in the `u`nfolds list in the Tip-document
        /// For EventStore, AccessStrategy.RollingSnapshots embeds these events every `batchSize` events
        | Snapshotted   of SnapshotData
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Config.EventCodec.gen<Event>, Config.EventCodec.genJsonElement<Event>

/// Types and mapping logic used maintain relevant State based on Events observed on the Todo List Stream
module Fold =

    /// Present state of the Todo List as inferred from the Events we've seen to date
    type State = { items : Events.ItemData list; nextId : int }
    /// State implied by the absence of any events on this stream
    let initial = { items = []; nextId = 0 }
    /// Compute State change implied by a given Event
    let evolve state = function
        | Events.Added item    -> { state with items = item :: state.items; nextId = state.nextId + 1 }
        | Events.Updated value -> { state with items = state.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Events.Deleted e     -> { state with items = state.items  |> List.filter (fun x -> x.id <> e.id) }
        | Events.Cleared e     -> { nextId = e.nextId; items = [] }
        | Events.Snapshotted s -> { nextId = s.nextId; items = List.ofArray s.items }
    /// Folds a set of events from the store into a given `state`
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared _ | Events.Snapshotted _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let toSnapshot state = Events.Snapshotted { nextId = state.nextId; items = Array.ofList state.items }

/// Properties that can be edited on a Todo List item
type Props = { order: int; title: string; completed: bool }

let mkItem id (value : Props) : Events.ItemData = { id = id; order = value.order; title = value.title; completed = value.completed }

let decideAdd value (state : Fold.State) =
    [ Events.Added (mkItem state.nextId value) ]

let decideUpdate itemId value (state : Fold.State) =
    let proposed = mkItem itemId value
    match state.items |> List.tryFind (function { id = id } -> id = itemId) with
    | Some current when current <> proposed -> [ Events.Updated proposed ]
    | _ -> []

let decideDelete id (state : Fold.State) =
    if state.items |> List.exists (fun x -> x.id = id) then [ Events.Deleted { id=id } ] else []

let decideClear (state : Fold.State) =
    if state.items |> List.isEmpty then [] else [ Events.Cleared { nextId = state.nextId } ]

/// A single Item in the Todo List
type View = { id: int; order: int; title: string; completed: bool }

let private render (item: Events.ItemData) : View =
    {   id = item.id
        order = item.order
        title = item.title
        completed = item.completed }

/// Defines operations that a Controller can perform on a Todo List
type Service internal (resolve : ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    (* READ *)

    /// List all open items
    member _.List clientId  : Async<View seq> =
        let decider = resolve clientId
        decider.Query(fun x -> seq { for x in x.items -> render x })

    /// Load details for a single specific item
    member _.TryGet(clientId, id) : Async<View option> =
        let decider = resolve clientId
        decider.Query(fun x -> x.items |> List.tryFind (fun x -> x.id = id) |> Option.map render)

    (* WRITE *)

    /// Execute the specified (blind write) command
    member _.Execute(clientId , command) : Async<unit> =
        let decider = resolve clientId
        decider.Transact command

    /// Create a single item
    member _.Add(clientId, props) =
        let decider = resolve clientId
        decider.Transact(decideAdd props)

    /// Update a single item
    member _.Update(clientId, id, props) =
        let decider = resolve clientId
        decider.Transact(decideUpdate id props)

    /// Delete a single item from the list
    member _.Delete(clientId, id) =
        let decider = resolve clientId
        decider.Transact(decideDelete id)

    /// Completely clear the Todo list
    member _.Clear(clientId) : Async<unit> =
        let decider = resolve clientId
        decider.Transact decideClear

    (* WRITE-READ *)

    /// Create a new ToDo List item; response contains the generated `id`
    member _.Create(clientId, template: Props) : Async<View> =
        let decider = resolve clientId
        decider.Transact(decideAdd template, fun s -> s.items |> List.head |> render)

    /// Update the specified item as referenced by the `item.id`
    member _.Patch(clientId, id: int, value: Props) : Async<View> =
        let decider = resolve clientId
        let echoUpdated id (s : Fold.State) = s.items |> List.find (fun x -> x.id = id)
        decider.Transact(decideUpdate id value, echoUpdated id >> render)

module Config =

    let private resolveCategory = function
#if (memoryStore || (!cosmos && !dynamo && !eventStore))
        | Config.Store.Memory store ->
            Config.Memory.create Events.codec Fold.initial Fold.fold store
#endif
//#if cosmos
        | Config.Store.Cosmos (context, cache) ->
            Config.Cosmos.createSnapshotted Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
//#if dynamo
        | Config.Store.Dynamo (context, cache) ->
            Config.Dynamo.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
//#if eventStore
        | Config.Store.Esdb (context, cache) ->
            Config.Esdb.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
    let create store = Service(fun id -> Config.resolveDecider (resolveCategory store) Category (streamId id))
