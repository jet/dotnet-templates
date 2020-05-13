module ConsumerTemplate.SkuSummary

let [<Literal>] Category = "SkuSummary"
let streamName (id : SkuId) = FsCodec.StreamName.create Category (SkuId.toString id)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemData =
        {   locationId : string
            messageIndex : int64
            picketTicketId : string
            poNumber : string
            reservedQuantity : int }
    type Event =
        | Ingested of ItemData
        | Snapshotted of ItemData[]
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Events.ItemData list
    module State =
        let equals (x : Events.ItemData) (y : Events.ItemData) =
            x.locationId = y.locationId
        let supersedes (x : Events.ItemData) (y : Events.ItemData) =
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
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let snapshot (x : State) : Events.Event = Events.Snapshotted (Array.ofList x)

type Command =
    | Consume of Events.ItemData list

let interpret command (state : Fold.State) =
    match command with
    | Consume updates ->
        [for x in updates do if x |> Fold.State.isNewOrUpdated state then yield Events.Ingested x]

type Service internal (resolve : SkuId -> Equinox.Stream<Events.Event, Fold.State>) =

    /// <returns>count of items</returns>
    member __.Ingest(skuId, items) : Async<int> =
        let stream = resolve skuId
        let executeWithCount command : Async<int> =
            let decide state =
                let events = interpret command state
                List.length events, events
            stream.Transact(decide)
        executeWithCount <| Consume items

    member __.Read skuId: Async<Events.ItemData list> =
        let stream = resolve skuId
        stream.Query id

let create resolve =
    let resolve skuId =
        let stream = resolve (streamName skuId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts=3)
    Service(resolve)

module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.Snapshot (Fold.isOrigin, Fold.snapshot)
    let create (context, cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
