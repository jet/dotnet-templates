module ConsumerTemplate.Todo

// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData = { id: int; order: int; title: string; completed: bool }
    type SummaryData = { items : ItemData[] }

    type Event =
        | Ingested of {| version: int64; value : SummaryData |}
        interface TypeShape.UnionContract.IUnionContract

module Folds =

    type State = { version : int64; value : Events.SummaryData option }
    let initial = { version = -1L; value = None }
    let evolve s = function
        | Events.Ingested e -> { version = e.version; value = Some e.value }
    let fold (state : State) : Events.Event seq -> State = Seq.fold evolve state
    let isOrigin = function _ -> true
    // A `transmute` function gets presented with:
    // a) events a command decided to generate (in it's `interpret`)
    // b) the state after applying them
    // and is expected to return:
    // a) the revised list of events to actually write as events in the stream
    // b) the snapshot(s) to put in the `u`nfolds list in the Tip
    //
    // This implementation means that every time `interpret` decides to write an `Ingested` event, we flip what would
    // normally happen (write a new event in a new document in the stream and update the snapshot so we can read it in one shot)
    // and use AccessStrategy.RollingUnfolds with this `transmute` function so we instead convey:
    // a) "don't actually write these events we just decided on in `interpret` [and don't insert a new event batch document]"
    // b) "can you treat these events as snapshots please"
    let transmute events _state : Events.Event list * Events.Event list =
        [],events

module Commands =
    type Command =
        | Consume of version: int64 * Events.SummaryData

    let decide command (state : Folds.State) =
        match command with
        | Consume (version,value) ->
            if state.version <= version then false,[] else
            true,[Events.Ingested {| version = version; value = value |}]

let [<Literal>]categoryId = "TodoSummary"

type Item = { id: int; order: int; title: string; completed: bool }

type Service(handlerLog, resolve, ?maxAttempts) =
    let (|AggregateId|) (clientId: ClientId) = Equinox.AggregateId(categoryId, ClientId.toStringN clientId)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(handlerLog, resolve id, maxAttempts = defaultArg maxAttempts 2)
    let execute (Stream stream) command : Async<bool> =
        stream.Transact(Commands.decide command)
    let query (Stream stream) (projection : Folds.State -> 't) : Async<'t> =
        stream.Query projection
    let render : Folds.State -> Item[] = function
        | { value = Some { items = xs} } ->
            [| for x in xs ->
                {   id = x.id
                    order = x.order
                    title = x.title
                    completed = x.completed } |]
        | _ -> [||]

    member __.Ingest clientId (version,value) : Async<bool> =
        execute clientId <| Commands.Consume (version,value)

    member __.Read clientId: Async<Item[]> =
        query clientId render