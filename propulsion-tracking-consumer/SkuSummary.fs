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

module Folds =

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

module Commands =

    type Command =
        | Consume of Events.ItemData list

    let interpret command (state : Folds.State) =
        match command with
        | Consume updates ->
            [for x in updates do if x |> Folds.State.isNewOrUpdated state then yield Events.Ingested x]

let [<Literal>] categoryId = "SkuSummary"

type Service(log, resolve, ?maxAttempts) =

    let (|AggregateId|) (id : SkuId) = Equinox.AggregateId(categoryId, SkuId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)

    let executeWithCount (Stream stream) command : Async<int> =
        let decide state =
            let events = Commands.interpret command state
            List.length events,events
        stream.Transact(decide)

    let query (Stream stream) (projection : Folds.State -> 't) : Async<'t> =
        stream.Query projection

    /// <returns>count of items</returns>
    member __.Ingest(skuId, items) : Async<int> =
        executeWithCount skuId <| Commands.Consume items

    member __.Read skuId: Async<Events.ItemData list> =
        query skuId id

module Cosmos =

    open Equinox.Cosmos // Everything until now is independent of a concrete store
    let private resolve (context,cache) =
        // We don't want to write any events, so here we supply the `transmute` function to teach it how to treat our events as snapshots
        let accessStrategy = AccessStrategy.Snapshot(Folds.isOrigin, Folds.snapshot)
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve
    let createService (context,cache) = Service(Serilog.Log.ForContext<Service>(), resolve (context,cache))