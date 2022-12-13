﻿module ConsumerTemplate.TodoSummary

let [<Literal>] Category = "TodoSummary"
let streamId = Equinox.StreamId.gen ClientId.toString

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData = { id: int; order: int; title: string; completed: bool }
    type SummaryData = { items : ItemData[] }
    type IngestedData = { version : int64; value : SummaryData }
    type Event =
        | Ingested of IngestedData
        interface TypeShape.UnionContract.IUnionContract
    let codec = Config.EventCodec.gen<Event>

module Fold =

    type State = { version : int64; value : Events.SummaryData option }
    let initial = { version = -1L; value = None }
    let evolve _state = function
        | Events.Ingested e -> { version = e.version; value = Some e.value }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let toSnapshot state = Events.Ingested { version = state.version; value = state.value.Value }

let ingest version value (state : Fold.State) =
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
type Service internal (resolve : ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.TryIngest(clientId, version, value) : Async<bool> =
        let decider = resolve clientId
        decider.Transact(ingest version value)

    member _.Read clientId: Async<Item[]> =
        let decider = resolve clientId
        decider.Query render

module Config =

    let private (|Category|) = function
        | Config.Store.Cosmos (context, cache) -> Config.Cosmos.createRollingState Events.codec Fold.initial Fold.fold Fold.toSnapshot (context, cache)
    let create (Category cat) = Service(streamId >> Config.createDecider cat Category) 
