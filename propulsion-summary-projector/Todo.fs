module ProjectorTemplate.Todo

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    /// Information we retain per Todo List entry
    type ItemData = { id: int; order: int; title: string; completed: bool }
    /// Events we keep in Todo-* streams
    type Event =
        | Added     of ItemData
        | Updated   of ItemData
        | Deleted   of {| id: int |}
        /// Cleared also `isOrigin` (see below) - if we see one of these, we know we don't need to look back any further
        | Cleared   of {| nextId: int |}
        /// For EventStore, AccessStrategy.RollingSnapshots embeds these events every `batchSize` events
        /// For Cosmos, AccessStrategy.Snapshot maintains this as an event in the `u`nfolds list in the Tip-document
        | Snapshot of {| nextId: int; items: ItemData[] |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

/// Types and mapping logic used maintain relevant State based on Events observed on the Todo List Stream
module Folds =

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
        | Events.Snapshot s -> { nextId = s.nextId; items = List.ofArray s.items }
    /// Folds a set of events from the store into a given `state`
    let fold (state : State) : Events.Event seq -> State = Seq.fold evolve state
    /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
    let isOrigin = function Events.Cleared _ | Events.Snapshot _ -> true | _ -> false
    /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
    let snapshot state = Events.Snapshot {| nextId = state.nextId; items = Array.ofList state.items |}
    /// Allows us to slkip producing summaries for events that we know won't result in an externally discernable change to the summary output
    let impliesStateChange = function Events.Snapshot _ -> false | _ -> true

let [<Literal>]categoryId = "Todos"

/// Defines operations that a Controller or Projector can perform on a Todo List
type Service(log, resolve, ?maxAttempts) =
    /// Maps a ClientId to the AggregateId that specifies the Stream in which the data for that client will be held
    let (|AggregateId|) (clientId: ClientId) = Equinox.AggregateId(categoryId, ClientId.toString clientId)

    /// Maps a ClientId to a Stream for the relevant stream
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)

    /// Establish the present state of the Stream, project from that as specified by `projection` (using QueryEx so we can determine the version in effect)
    let queryEx (Stream stream) (projection : Folds.State -> 't) : Async<int64*'t> =
        stream.QueryEx(fun v s -> v, projection s)

    /// Load and render the state
    member __.QueryWithVersion(clientId, render : Folds.State -> 'res) : Async<int64*'res> =
        queryEx clientId render

module Repository =
    open Equinox.Cosmos // Everything until now is independent of a concrete store
    let private resolve cache context =
        let accessStrategy = AccessStrategy.Snapshot (Folds.isOrigin,Folds.snapshot)
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve
    let createService cache context = Service(Serilog.Log.ForContext<Service>(), resolve cache context)