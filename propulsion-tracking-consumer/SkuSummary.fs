module ConsumerTemplate.SkuSummary

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
    let [<Literal>] category = "SkuSummary"
    let (|For|) (id : SkuId) = Equinox.AggregateId(category, SkuId.toString id)

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

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

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

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)

module Cosmos =

    open Equinox.Cosmos // Everything until now is independent of a concrete store
    let private resolve (context, cache) =
        // We don't want to write any events, so here we supply the `transmute` function to teach it how to treat our events as snapshots
        let accessStrategy = AccessStrategy.Snapshot(Fold.isOrigin, Fold.snapshot)
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
    let create (context, cache) = create (resolve (context, cache))