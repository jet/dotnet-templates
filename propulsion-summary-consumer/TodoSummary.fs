module ConsumerTemplate.TodoSummary

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData = { id: int; order: int; title: string; completed: bool }
    type SummaryData = { items : ItemData[] }
    type IngestedData = { version : int64; value : SummaryData }
    type Event =
        | Ingested of IngestedData
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Folds =

    type State = { version : int64; value : Events.SummaryData option }
    let initial = { version = -1L; value = None }
    let evolve _state = function
        | Events.Ingested e -> { version = e.version; value = Some e.value }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let snapshot state = Events.Ingested { version = state.version; value = state.value.Value }
    let accessStrategy = Equinox.Cosmos.AccessStrategy.RollingState snapshot

module Commands =
    type Command =
        | Consume of version : int64 * value : Events.SummaryData

    let decide command (state : Folds.State) =
        match command with
        | Consume (version,value) ->
            if state.version <= version then false,[] else
            true,[Events.Ingested { version = version; value = value }]

type Item = { id: int; order: int; title: string; completed: bool }
let render : Folds.State -> Item[] = function
    | { value = Some { items = xs} } ->
        [| for x in xs ->
            {   id = x.id
                order = x.order
                title = x.title
                completed = x.completed } |]
    | _ -> [||]

let [<Literal>] categoryId = "TodoSummary"

/// Defines the operations that the Read side of a Controller and/or the Ingester can perform on the 'aggregate'
type Service(log, resolve, ?maxAttempts) =

    let (|AggregateId|) (clientId: ClientId) = Equinox.AggregateId(categoryId, ClientId.toString clientId)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)

    let execute (Stream stream) command : Async<bool> =
        stream.Transact(Commands.decide command)

    let query (Stream stream) (projection : Folds.State -> 't) : Async<'t> =
        stream.Query projection

    member __.Ingest(clientId, version, value) : Async<bool> =
        execute clientId <| Commands.Consume (version,value)

    member __.Read clientId: Async<Item[]> =
        query clientId render

module Cosmos =

    open Equinox.Cosmos // Everything until now is independent of a concrete store
    let private resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, Folds.accessStrategy).Resolve
    let createService (context,cache) = Service(Serilog.Log.ForContext<Service>(), resolve (context,cache))