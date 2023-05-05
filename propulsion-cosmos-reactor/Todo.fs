module ReactorTemplate.Todo

let [<Literal>] Category = "Todos"
let streamId = Equinox.StreamId.gen ClientId.toString
let [<return: Struct>] (|StreamName|_|) = function FsCodec.StreamName.CategoryAndId (Category, ClientId.Parse clientId) -> ValueSome clientId | _ -> ValueNone

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData =     { id : int; order : int; title : string; completed : bool }
    type DeletedData =  { id : int }
    type ClearedData =  { nextId : int }
    type SnapshotData = { nextId : int; items : ItemData[] }
    type Event =
        | Added         of ItemData
        | Updated       of ItemData
        | Deleted       of DeletedData
        | Cleared       of ClearedData
        | Snapshotted   of SnapshotData
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.genJsonElement<Event>

module Reactions =

    let categories = [| Category |]
    
    /// Allows us to skip producing summaries for events that we know won't result in an externally discernable change to the summary output
    let private impliesStateChange = function Events.Snapshotted _ -> false | _ -> true
    
    let dec = Streams.Codec.gen<Events.Event>
    let [<return: Struct>] (|ImpliesStateChange|_|) = function
        | struct (StreamName clientId, _) & Streams.Decode dec events when Array.exists impliesStateChange events -> ValueSome clientId
        | _ -> ValueNone

/// Types and mapping logic used maintain relevant State based on Events observed on the Todo List Stream
module Fold =

    /// Present state of the Todo List as inferred from the Events we've seen to date
    type State = { items : Events.ItemData list; nextId : int }
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
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared _ | Events.Snapshotted _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let toSnapshot state = Events.Snapshotted { nextId = state.nextId; items = Array.ofList state.items }

/// Defines operations that a Controller or Projector can perform on a Todo List
type Service internal (resolve : ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Load and render the state
    member _.QueryWithVersion(clientId, render : Fold.State -> 'res) : Async<int64*'res> =
        let decider = resolve clientId
        // Establish the present state of the Stream, project from that (using QueryEx so we can determine the version in effect)
        decider.QueryEx(fun c -> c.Version, render c.State)

module Factory =

    let private (|Category|) = function
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
    let create (Category cat) = Service(streamId >> Store.createDecider cat Category)
    
