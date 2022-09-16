module TodoBackendTemplate.Aggregate

let [<Literal>] Category = "Aggregate"
let streamName (id: string) = struct (Category, id)

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type SnapshottedData = { happened: bool }

    type Event =
        | Happened
        | Snapshotted of SnapshottedData
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Config.EventCodec.gen<Event>, Config.EventCodec.genJsonElement<Event>

module Fold =

    type State = { happened: bool }
    let initial = { happened = false }
    let evolve s = function
        | Events.Happened -> { happened = true }
        | Events.Snapshotted e -> { happened = e.happened}
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot state = Events.Snapshotted { happened = state.happened }

let interpretMarkDone (state : Fold.State) =
    if state.happened then [] else [Events.Happened]

type View = { sorted : bool }

type Service internal (resolve : string -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Read the present state
    // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
    member _.Read clientId : Async<View> =
        let decider = resolve clientId
        decider.Query(fun s -> { sorted = s.happened })

    /// Execute the specified command
    member _.MarkDone(clientId, command) : Async<unit> =
        let decider = resolve clientId
        decider.Transact(interpretMarkDone)

module Config =

    let private (|Category|) = function
#if (memoryStore || (!cosmos && !dynamo && !eventStore))
        | Config.Store.Memory store ->
            Config.Memory.create Events.codec Fold.initial Fold.fold store
#endif
//#endif
//#if cosmos
        | Config.Store.Cosmos (context, cache) ->
            Config.Cosmos.createSnapshotted Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
//#if dynamo
        | Config.Store.Dynamo (context, cache) ->
            Config.Dynamo.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
//#if eventStore
        | Config.Store.Esdb (context, cache) ->
            Config.Esdb.createSnapshotted Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
    let create (Category cat) = Service(streamName >> Config.resolveDecider cat)
