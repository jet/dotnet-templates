﻿module ConsumerTemplate.SkuSummary

module private Stream = 
    let [<Literal>] Category = "SkuSummary"
    let id = FsCodec.StreamId.gen SkuId.toString
    let (|Decode|) = FsCodec.StreamId.dec SkuId.parse
module Reactions =
    let (|SkuId|) = function FsCodec.StreamName.Split (_, Stream.Decode skuId) -> skuId

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData =
        {   locationId: string
            messageIndex: int64
            picketTicketId: string
            poNumber: string
            reservedQuantity: int }
    type Event =
        | Ingested of ItemData
        | Snapshotted of ItemData[]
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>

module Fold =

    type State = Events.ItemData list
    module State =
        let equals (x: Events.ItemData) (y: Events.ItemData) =
            x.locationId = y.locationId
        let supersedes (x: Events.ItemData) (y: Events.ItemData) =
            equals x y
            && y.messageIndex > x.messageIndex
            && y.reservedQuantity <> x.reservedQuantity
        let isNewOrUpdated state x =
            not (state |> List.exists (fun y -> equals y x || supersedes y x))

    let initial = []
    // Defines a valid starting point for a fold, i.e. the point beyond which we don't need any preceding events to buid the state
    let isOrigin = function
        | Events.Snapshotted _ -> true // Yes, a snapshot is enough info
        | Events.Ingested _ -> false
    let evolve state = function
        | Events.Ingested e -> e :: state
        | Events.Snapshotted items -> List.ofArray items
    let fold: State -> Events.Event seq -> State = Seq.fold evolve
    let toSnapshot (x: State): Events.Event = Events.Snapshotted (Array.ofList x)

let ingest (updates: Events.ItemData list) (state: Fold.State) = [|
    for x in updates do
        if x |> Fold.State.isNewOrUpdated state then
            Events.Ingested x |]

type Service internal (resolve: SkuId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// <returns>count of items</returns>
    member _.Ingest(skuId, items): Async<int> =
        let decider = resolve skuId
        let decide state =
            let events = ingest items state
            Array.length events, events
        decider.Transact(decide)

    member _.Read skuId: Async<Events.ItemData list> =
        let decider = resolve skuId
        decider.Query id

module Factory =

    let private (|Category|) = function
        | Store.Config.Cosmos (context, cache) -> Store.Cosmos.createSnapshotted Stream.Category Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
    let create (Category cat) = Service(Stream.id >> Store.createDecider cat)
