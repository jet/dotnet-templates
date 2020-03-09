module TodoBackendTemplate.Todo

let [<Literal>] Category = "Todos"
/// Maps a ClientId to the StreamName where data for that client will be held
let streamName (clientId: ClientId) = FsCodec.StreamName.create Category (ClientId.toString clientId)

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
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

/// Types and mapping logic used maintain relevant State based on Events observed on the Todo List Stream
module Fold =

    /// Present state of the Todo List as inferred from the Events we've seen to date
    type State = { items : Events.ItemData list; nextId : int }
    /// State implied by the absence of any events on this stream
    let initial = { items = []; nextId = 0 }
    /// Compute State change implied by a given Event
    let evolve state = function
        | Events.Added item -> { state with items = item :: state.items; nextId = state.nextId + 1 }
        | Events.Updated value -> { state with items = state.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Events.Deleted e -> { state with items = state.items  |> List.filter (fun x -> x.id <> e.id) }
        | Events.Cleared e -> { nextId = e.nextId; items = [] }
        | Events.Snapshotted s -> { nextId = s.nextId; items = List.ofArray s.items }
    /// Folds a set of events from the store into a given `state`
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared _ | Events.Snapshotted _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let snapshot state = Events.Snapshotted { nextId = state.nextId; items = Array.ofList state.items }

/// Properties that can be edited on a Todo List item
type Props = { order: int; title: string; completed: bool }

/// Defines the operations a caller can perform on a Todo List
type Command =
    /// Create a single item
    | Add of Props
    /// Update a single item
    | Update of id: int * Props
    /// Delete a single item from the list
    | Delete of id: int
    /// Complete clear the Todo list
    | Clear

/// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream
let interpret c (state : Fold.State) =
    let mkItem id (value: Props): Events.ItemData = { id = id; order=value.order; title=value.title; completed=value.completed }
    match c with
    | Add value -> [Events.Added (mkItem state.nextId value)]
    | Update (itemId, value) ->
        let proposed = mkItem itemId value
        match state.items |> List.tryFind (function { id = id } -> id = itemId) with
        | Some current when current <> proposed -> [Events.Updated proposed]
        | _ -> []
    | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Events.Deleted { id=id }] else []
    | Clear -> if state.items |> List.isEmpty then [] else [Events.Cleared { nextId = state.nextId }]

/// A single Item in the Todo List
type View = { id: int; order: int; title: string; completed: bool }

/// Defines operations that a Controller can perform on a Todo List
type Service internal (log, resolve, maxAttempts) =

    let resolve clientId =
        let stream = resolve (streamName clientId)
        Equinox.Stream(log, stream, maxAttempts = maxAttempts)

    let execute clientId command =
        let stream = resolve clientId
        stream.Transact(interpret command)
    let query clientId projection =
        let stream = resolve clientId
        stream.Query projection
    let handle clientId command =
        let stream = resolve clientId
        stream.Transact(fun state ->
            let events = interpret command state
            let state' = Fold.fold state events
            state'.items,events)

    let render (item: Events.ItemData) : View =
        {   id = item.id
            order = item.order
            title = item.title
            completed = item.completed }

    (* READ *)

    /// List all open items
    member __.List clientId  : Async<View seq> =
        query clientId (fun x -> seq { for x in x.items -> render x })

    /// Load details for a single specific item
    member __.TryGet(clientId, id) : Async<View option> =
        query clientId (fun x -> x.items |> List.tryFind (fun x -> x.id = id) |> Option.map render)

    (* WRITE *)

    /// Execute the specified (blind write) command 
    member __.Execute(clientId , command) : Async<unit> =
        execute clientId command

    (* WRITE-READ *)

    /// Create a new ToDo List item; response contains the generated `id`
    member __.Create(clientId , template: Props) : Async<View> = async {
        let! state' = handle clientId (Add template)
        return List.head state' |> render }

    /// Update the specified item as referenced by the `item.id`
    member __.Patch(clientId, id: int, value: Props) : Async<View> = async {
        let! state' = handle clientId (Update (id, value))
        return state' |> List.find (fun x -> x.id = id) |> render}

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)