module TodoBackendTemplate.Aggregate

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type CompactedData = { happened: bool }

    type Event =
        | Happened
        | Compacted of CompactedData
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let (|For|) (id: string) = Equinox.AggregateId("Aggregate", id)

module Fold =

    type State = { happened: bool }
    let initial = { happened = false }
    let evolve s = function
        | Events.Happened -> { happened = true }
        | Events.Compacted e -> { happened = e.happened} 
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Compacted _ -> true | _ -> false
    let compact state = Events.Compacted { happened = state.happened }

type Command =
    | MakeItSo

let interpret c (state : Fold.State) =
    match c with
    | MakeItSo -> if state.happened then [] else [Events.Happened]

type View = { sorted : bool }

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

    /// Read the present state
    // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
    member __.Read clientId : Async<View> =
        let stream = resolve clientId
        stream.Query(fun s -> { sorted = s.happened })

    /// Execute the specified command 
    member __.Execute(clientId, command) : Async<unit> =
        let stream = resolve clientId
        stream.Transact(interpret command)

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)