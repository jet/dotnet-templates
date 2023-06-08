module ReactorTemplate.Todo

open Propulsion.Internal

let [<Literal>] Category = "Todos"
let streamId = Equinox.StreamId.gen ClientId.toString
let [<return: Struct>] (|StreamName|_|) = function FsCodec.StreamName.CategoryAndId (Category, ClientId.Parse clientId) -> ValueSome clientId | _ -> ValueNone

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData =     { id: int; order: int; title: string; completed: bool }
    type DeletedData =  { id: int }
    type ClearedData =  { nextId: int }
    type SnapshotData = { nextId: int; items: ItemData[] }
    type Event =
        | Added         of ItemData
        | Updated       of ItemData
        | Deleted       of DeletedData
        | Cleared       of ClearedData
        | Snapshotted   of SnapshotData
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Reactions =

    let categories = [| Category |]
    
    /// Allows us to skip producing summaries for events that we know won't result in an externally discernable change to the summary output
    let private impliesStateChange = function Events.Snapshotted _ -> false | _ -> true
    
    let private dec = Streams.Codec.gen<Events.Event>
    let [<return: Struct>] private (|Parse|_|) = function
        | struct (StreamName clientId, _) & Streams.Decode dec events -> ValueSome struct (clientId, events)
        | _ -> ValueNone
    let (|ImpliesStateChange|NoStateChange|NotApplicable|) = function
        | Parse (clientId, events) ->
            if events |> Array.exists impliesStateChange then ImpliesStateChange (clientId, events.Length)
            else NoStateChange events.Length
        | _, events -> NotApplicable events.Length

/// Types and mapping logic used maintain relevant State based on Events observed on the Todo List Stream
module Fold =

    /// Present state of the Todo List as inferred from the Events we've seen to date
    type State = { items: Events.ItemData list; nextId: int }
    /// State implied by the absence of any events on this stream
    let initial = { items = []; nextId = 0 }
    /// Compute State change implied by a giveC:\Users\f0f00db\Projects\dotnet-templates\propulsion-summary-projector\Todo.fsn Event
    let evolve s = function
        | Events.Added item -> { s with items = item :: s.items; nextId = s.nextId + 1 }
        | Events.Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Events.Deleted e -> { s with items = s.items |> List.filter (fun x -> x.id <> e.id) }
        | Events.Cleared e -> { nextId = e.nextId; items = [] }
        | Events.Snapshotted s -> { nextId = s.nextId; items = List.ofArray s.items }
    /// Folds a set of events from the store into a given `state`
    let fold: State -> Events.Event seq -> State = Seq.fold evolve
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared _ | Events.Snapshotted _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let toSnapshot state = Events.Snapshotted { nextId = state.nextId; items = Array.ofList state.items }

/// Defines operations that a Controller or Projector can perform on a Todo List
type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Load and render the state
    member _.QueryWithVersion(clientId, render: Fold.State -> 'res): Async<int64*'res> =
        let decider = resolve clientId
        // Establish the present state of the Stream, project from that (using QueryEx so we can determine the version in effect)
        decider.QueryEx(fun c -> c.Version, render c.State)

module Factory =

    let private (|Category|) = function
        | Store.Context.Dynamo (context, cache) -> Store.Dynamo.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
#if !(sourceKafka && kafka)
        | Store.Context.Esdb (context, cache) ->   Store.Esdb.create Events.codec Fold.initial Fold.fold (context, cache)
        | Store.Context.Sss (context, cache) ->    Store.Sss.create Events.codec Fold.initial Fold.fold (context, cache)
#endif
    let create (Category cat) = Service(streamId >> Store.createDecider cat Category)
