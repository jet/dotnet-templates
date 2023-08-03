module ReactorTemplate.TodoSummary

module private Stream =
    let [<Literal>] Category = "TodoSummary"
    let id = Equinox.StreamId.gen ClientId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData = { id: int; order: int; title: string; completed: bool }
    type SummaryData = { items: ItemData[] }
    type IngestedData = { version: int64; value: SummaryData }
    type Event =
        | Ingested of IngestedData
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Fold =

    type State = { version: int64; value: Events.SummaryData option }
    let initial = { version = -1L; value = None }
    let evolve _state = function
        | Events.Ingested e -> { version = e.version; value = Some e.value }
    let fold: State -> Events.Event seq -> State = Seq.fold evolve
    let toSnapshot state = Events.Ingested { version = state.version; value = state.value.Value }

let ingest version value (state: Fold.State) =
    if state.version >= version then false, [||] else
    true, [| Events.Ingested { version = version; value = value } |]

type Item = { id: int; order: int; title: string; completed: bool }
let render: Fold.State -> Item[] = function
    | { value = Some { items = xs} } ->
        [| for x in xs ->
            {   id = x.id
                order = x.order
                title = x.title
                completed = x.completed } |]
    | _ -> [||]

/// Defines the operations that the Read side of a Controller and/or the Ingester can perform on the 'aggregate'
type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Returns false if the ingestion was rejected due to being an older version of the data than is presently being held
    member _.TryIngest(clientId, version, value): Async<bool> =
        let decider = resolve clientId
        decider.Transact(ingest version value)

    member _.Read(clientId): Async<Item[]> =
        let decider = resolve clientId
        decider.Query render

module Factory =

    let private (|Category|) = function
        | Store.Context.Cosmos (context, cache) -> Store.Cosmos.createRollingState Stream.Category Events.codecJe Fold.initial Fold.fold Fold.toSnapshot (context, cache)
        | Store.Context.Dynamo (context, cache) -> Store.Dynamo.createRollingState Stream.Category Events.codec Fold.initial Fold.fold Fold.toSnapshot (context, cache)
#if !(sourceKafka && kafka)
        | Store.Context.Esdb (context, cache) ->   Store.Esdb.create Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
        | Store.Context.Sss (context, cache) ->    Store.Sss.create Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
#endif
    let create (Category cat) = Service(Stream.id >> Store.createDecider cat)
