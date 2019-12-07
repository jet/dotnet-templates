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
    let [<Literal>] category = "TodoSummary"
    let (|For|) (clientId: ClientId) = Equinox.AggregateId(category, ClientId.toString clientId)

module Fold =

    type State = { version : int64; value : Events.SummaryData option }
    let initial = { version = -1L; value = None }
    let evolve _state = function
        | Events.Ingested e -> { version = e.version; value = Some e.value }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let snapshot state = Events.Ingested { version = state.version; value = state.value.Value }
    let accessStrategy = Equinox.Cosmos.AccessStrategy.RollingState snapshot

type Command =
    | Consume of version : int64 * value : Events.SummaryData

let decide command (state : Fold.State) =
    match command with
    | Consume (version, value) ->
        if state.version <= version then false, [] else
        true, [Events.Ingested { version = version; value = value }]

type Item = { id: int; order: int; title: string; completed: bool }
let render : Fold.State -> Item[] = function
    | { value = Some { items = xs} } ->
        [| for x in xs ->
            {   id = x.id
                order = x.order
                title = x.title
                completed = x.completed } |]
    | _ -> [||]

/// Defines the operations that the Read side of a Controller and/or the Ingester can perform on the 'aggregate'
type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

    member __.Ingest(clientId, version, value) : Async<bool> =
        let stream = resolve clientId
        stream.Transact(decide (Consume (version, value)))

    member __.Read clientId: Async<Item[]> =
        let stream = resolve clientId
        stream.Query render

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)

module Cosmos =

    open Equinox.Cosmos // Everything until now is independent of a concrete store
    let private resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, Fold.accessStrategy).Resolve
    let create (context, cache) = create (resolve (context, cache))