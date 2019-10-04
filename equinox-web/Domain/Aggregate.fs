module TodoBackendTemplate.Aggregate

// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type CompactedData = { happened: bool }

    type Event =
        | Happened
        | Compacted of CompactedData
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>(rejectNullaryCases=false)

module Folds =

    type State = { happened: bool }
    let initial = { happened = false }
    let evolve s = function
        | Events.Happened -> { happened = true }
        | Events.Compacted e -> { happened = e.happened} 
    let fold (state : State) : Events.Event seq -> State = Seq.fold evolve state
    let isOrigin = function Events.Compacted _ -> true | _ -> false
    let compact state = Events.Compacted { happened = state.happened }

module Commands =

    type Command =
        | MakeItSo

    let interpret c (state : Folds.State) =
        match c with
        | MakeItSo -> if state.happened then [] else [Events.Happened]

type View = { sorted : bool }

type Service(handlerLog, resolve, ?maxAttempts) =

    let (|AggregateId|) (id: string) = Equinox.AggregateId("Aggregate", id)
    let (|Stream|) (AggregateId id) = Equinox.Stream(handlerLog, resolve id, maxAttempts = defaultArg maxAttempts 2)

    /// Execute `command`, syncing any events decided upon
    let execute (Stream stream) command : Async<unit> =
        stream.Transact(Commands.interpret command)
    /// Establish the present state of the Stream, project from that as specified by `projection`
    let query (Stream stream) (projection : Folds.State -> 't) : Async<'t> =
        stream.Query projection

    let render (s: Folds.State) : View =
        {   sorted = s.happened }

    /// Read the present state
    // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
    member __.Read clientId : Async<View> =
        query clientId render

    /// Execute the specified command 
    member __.Execute(clientId, command) : Async<unit> =
        execute clientId command