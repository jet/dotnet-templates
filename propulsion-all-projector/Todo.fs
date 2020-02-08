module AllTemplate.Todo

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] CategoryId = "Todos"
    let (|ForClientId|) (id : ClientId) = FsCodec.StreamName.create CategoryId (ClientId.toString id)
    let (|MatchesCategory|_|) = function
        | FsCodec.StreamName.CategoryAndId (CategoryId, ClientId.Parse clientId) -> Some clientId
        | _ -> None

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
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let (|Decode|) (stream, span : Propulsion.Streams.StreamSpan<_>) =
        span.events |> Seq.choose (EventCodec.tryDecode codec stream)
    let (|Match|_|) = function
        | (MatchesCategory clientId, _) & (Decode events) -> Some (clientId, events)
        | _ -> None

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
        | Events.Deleted e -> { s with items = s.items  |> List.filter (fun x -> x.id <> e.id) }
        | Events.Cleared e -> { nextId = e.nextId; items = [] }
        | Events.Snapshotted s -> { nextId = s.nextId; items = List.ofArray s.items }
    /// Folds a set of events from the store into a given `state`
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared _ | Events.Snapshotted _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let snapshot state = Events.Snapshotted { nextId = state.nextId; items = Array.ofList state.items }
    /// Allows us to skip producing summaries for events that we know won't result in an externally discernable change to the summary output
    let impliesStateChange = function Events.Snapshotted _ -> false | _ -> true

/// Defines operations that a Controller or Projector can perform on a Todo List
type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.ForClientId id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

    /// Load and render the state
    member __.QueryWithVersion(clientId, render : Fold.State -> 'res) : Async<int64*'res> =
        let stream = resolve clientId
        // Establish the present state of the Stream, project from that (using QueryEx so we can determine the version in effect)
        stream.QueryEx(fun v s -> v, render s)

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)

module EventStore =

    open Equinox.EventStore // Everything until now is independent of a concrete store
    let private resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy).Resolve
    let create (context, cache) = resolve (context, cache) |> create