module TodoBackendTemplate.Aggregate

module private Stream =
    let [<Literal>] Category = "Aggregate"
    let id = FsCodec.StreamId.gen id

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type SnapshottedData = { happened: bool }

    type Event =
        | Happened
        | Snapshotted of SnapshottedData
        interface TypeShape.UnionContract.IUnionContract
    let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

module Fold =

    type State = { happened: bool }
    let initial = { happened = false }
    let evolve s = function
        | Events.Happened -> { happened = true }
        | Events.Snapshotted e -> { happened = e.happened}
    let fold: State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let toSnapshot state = Events.Snapshotted { happened = state.happened }

let interpretMarkDone (state: Fold.State) = [|
    if not state.happened then Events.Happened |]

type View = { sorted: bool }

type Service internal (resolve: string -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Read the present state
    // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
    member _.Read clientId: Async<View> =
        let decider = resolve clientId
        decider.Query(fun s -> { sorted = s.happened })

    /// Execute the specified command
    member _.MarkDone(clientId): Async<unit> =
        let decider = resolve clientId
        decider.Transact(interpretMarkDone)

module Factory =

    let private (|Category|) = function
#if (memoryStore || (!cosmos && !dynamo && !eventStore))
        | Store.Config.Memory store ->
            Store.Memory.create Stream.Category Events.codec Fold.initial Fold.fold store
#endif
//#endif
//#if cosmos
        | Store.Config.Cosmos (context, cache) ->
            Store.Cosmos.createSnapshotted Stream.Category Events.codecJe Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
//#if dynamo
        | Store.Config.Dynamo (context, cache) ->
            Store.Dynamo.createSnapshotted Stream.Category Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
//#if eventStore
        | Store.Config.Esdb (context, cache) ->
            Store.Esdb.createSnapshotted Stream.Category Events.codec Fold.initial Fold.fold (Fold.isOrigin, Fold.toSnapshot) (context, cache)
//#endif
    let create (Category cat) = Service(Stream.id >> Store.resolveDecider cat)
