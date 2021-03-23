module TodoBackendTemplate.Aggregate

let [<Literal>] Category = "Aggregate"
let streamName (id: string) = FsCodec.StreamName.create Category id

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type SnapshottedData = { happened: bool }

    type Event =
        | Happened
        | Snapshotted of SnapshottedData
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = { happened: bool }
    let initial = { happened = false }
    let evolve s = function
        | Events.Happened -> { happened = true }
        | Events.Snapshotted e -> { happened = e.happened}
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot state = Events.Snapshotted { happened = state.happened }

type Command =
    | MakeItSo

let interpret c (state : Fold.State) =
    match c with
    | MakeItSo -> if state.happened then [] else [Events.Happened]

type View = { sorted : bool }

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =

    /// Read the present state
    // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
    member _.Read clientId : Async<View> =
        let decider = resolve clientId
        decider.Query(fun s -> { sorted = s.happened })

    /// Execute the specified command 
    member _.Execute(clientId, command) : Async<unit> =
        let decider = resolve clientId
        decider.Transact(interpret command)

let create resolveStream =
    let resolve id =
        let stream = resolveStream (streamName id)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts=3)
    Service(resolve)