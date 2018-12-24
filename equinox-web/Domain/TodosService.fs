module TodoBackend.Todo

// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
module Events =

    /// Information we retain per Todo List entry
    type ItemData = { id: int; order: int; title: string; completed: bool }
    /// Events we keep in Todo-* streams
    type Event =
        | Added     of ItemData
        | Updated   of ItemData
        | Deleted   of int
        /// Cleared also `isOrigin` (see below) - if we see one of these, we know we don't need to look back any further
        | Cleared
        /// For EventStore, AccessStrategy.RollingSnapshots embeds these events every `batchSize` events
        | Compacted of ItemData[]
        interface TypeShape.UnionContract.IUnionContract

/// Types and mapping logic used maintain relevant State based on Events observed on the Todo List Stream
module Folds =

    /// Present state of the Todo List as inferred from the Events we've seen to date
    type State = { items : Events.ItemData list; nextId : int }
    /// State implied by the absence of any events on this stream
    let initial = { items = []; nextId = 0 }
    /// Compute State change implied by a given Event
    let evolve s = function
        | Events.Added item -> { s with items = item :: s.items; nextId = s.nextId + 1 }
        | Events.Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Events.Deleted id -> { s with items = s.items  |> List.filter (fun x -> x.id <> id) }
        | Events.Cleared -> { s with items = [] }
        | Events.Compacted items -> { s with items = List.ofArray items }
    /// Folds a set of events from the store into a given `state`
    let fold (state : State) : Events.Event seq -> State = Seq.fold evolve state
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared | Events.Compacted _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let compact state = Events.Compacted (Array.ofList state.items)

/// Properties that can be edited on a Todo List item
type Props = { order: int; title: string; completed: bool }

/// Defines the decion process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream 
module Commands =

    /// Defines the operations a caller can perform on a Todo List
    type Command =
        /// Create a single item
        | Add of Props
        /// Update a single item
        | Update of id: int * Props
        /// Delete a single item from the list
        | Delete of id: int
        /// Complete clear the todo list
        | Clear

    let interpret c (state : Folds.State) =
        let mkItem id (value: Props): Events.ItemData = { id = id; order=value.order; title=value.title; completed=value.completed }
        match c with
        | Add value -> [Events.Added (mkItem state.nextId value)]
        | Update (itemId,value) ->
            let proposed = mkItem itemId value
            match state.items |> List.tryFind (function { id = id } -> id = itemId) with
            | Some current when current <> proposed -> [Events.Updated proposed]
            | _ -> []
        | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Events.Deleted id] else []
        | Clear -> if state.items |> List.isEmpty then [] else [Events.Cleared]

/// Defines low level stream operations relevant to the Todo Stream in terms of Command and Events
type Handler(log, stream, ?maxAttempts) =

    let inner = Equinox.Handler(Folds.fold, log, stream, maxAttempts = defaultArg maxAttempts 2)

    /// Execute `command`; does not emit the post state
    member __.Execute command : Async<unit> =
        inner.Decide <| fun ctx ->
            ctx.Execute (Commands.interpret command)
    /// Handle `command`, return the items after the command's intent has been applied to the stream
    member __.Handle command : Async<Events.ItemData list> =
        inner.Decide <| fun ctx ->
            ctx.Execute (Commands.interpret command)
            ctx.State.items
    /// Establish the present state of the Stream, project from that as specified by `projection`
    member __.Query(projection : Folds.State -> 't) : Async<'t> =
        inner.Query projection

/// A single Item in the Todo List
type View = { id: int; order: int; title: string; completed: bool }

/// Defines operations that a Controller can perform on a Todo List
type Service(handlerLog, resolve) =
    
    /// Maps a ClientId to the CatId that specifies the Stream in whehch the data for that client will be held
    let (|CategoryId|) (clientId: ClientId) = Equinox.CatId("Todos", if obj.ReferenceEquals(clientId,null) then "1" else clientId.Value)
    
    /// Maps a ClientId to Handler for the relevant stream
    let (|Stream|) (CategoryId catId) = Handler(handlerLog, resolve catId)

    let render (item: Events.ItemData) : View =
        {   id = item.id
            order = item.order
            title = item.title
            completed = item.completed }

    (* READ *)

    /// List all open items
    member __.List(Stream stream) : Async<View seq> =
        stream.Query (fun x -> seq { for x in x.items -> render x })

    /// Load details for a single specific item
    member __.TryGet(Stream stream, id) : Async<View option> =
        stream.Query (fun x -> x.items |> List.tryFind (fun x -> x.id = id) |> Option.map render)

    (* WRITE *)

    /// Execute the specified (blind write) command 
    member __.Execute(Stream stream, command) : Async<unit> =
        stream.Execute command

    (* WRITE-READ *)

    /// Create a new ToDo List item; response contains the generated `id`
    member __.Create(Stream stream, template: Props) : Async<View> = async {
        let! state' = stream.Handle(Commands.Add template)
        return List.head state' |> render }

    /// Update the specified item as referenced by the `item.id`
    member __.Patch(Stream stream, id: int, value: Props) : Async<View> = async {
        let! state' = stream.Handle(Commands.Update (id, value))
        return state' |> List.find (fun x -> x.id = id) |> render}