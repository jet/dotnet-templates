module ReactorTemplate.TodoSummary

let [<Literal>] Category = "TodoSummary"
let streamName (clientId: ClientId) = FsCodec.StreamName.create Category (ClientId.toString clientId)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData = { id: int; order: int; title: string; completed: bool }
    type SummaryData = { items : ItemData[] }
    type IngestedData = { version : int64; value : SummaryData }
    type Event =
        | Ingested of IngestedData
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = { version : int64; value : Events.SummaryData option }
    let initial = { version = -1L; value = None }
    let evolve _state = function
        | Events.Ingested e -> { version = e.version; value = Some e.value }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let snapshot state = Events.Ingested { version = state.version; value = state.value.Value }

// TODO collapse this unless you actually end up with >1 kind of ingestion Command
type Command =
    | Consume of version : int64 * value : Events.SummaryData

let decide command (state : Fold.State) =
    match command with
    | Consume (version, value) ->
        if state.version >= version then false, [] else
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
type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =

    /// Returns false if the ingestion was rejected due to being an older version of the data than is presently being held
    member __.TryIngest(clientId, version, value) : Async<bool> =
        let stream = resolve clientId
        stream.Transact(decide (Consume (version, value)))

    member __.Read clientId: Async<Item[]> =
        let stream = resolve clientId
        stream.Query render

let create resolve =
    let resolve clientId =
        let stream = resolve (streamName clientId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts=3)
    Service(resolve)

//#if multiSource
module EventStore =

    let create (context, cache) =
        let cacheStrategy = Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy)
        create resolver.Resolve

//#endif
module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.RollingState Fold.snapshot
    let create (context, cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
