module TodoBackendTemplate.Aggregate

// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Compacted = { happened: bool }

    type Event =
        | Happened
        | Compacted of Compacted
        interface TypeShape.UnionContract.IUnionContract

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

type Handler(log, stream, ?maxAttempts) =

    let inner = Equinox.Handler(Folds.fold, log, stream, maxAttempts = defaultArg maxAttempts 2)

    /// Execute `command`, syncing any events decided upon
    member __.Execute command : Async<unit> =
        inner.Decide <| fun ctx ->
            ctx.Execute (Commands.interpret command)
    /// Establish the present state of the Stream, project from that as specified by `projection`
    member __.Query(projection : Folds.State -> 't) : Async<'t> =
        inner.Query projection

type View = { sorted : bool }

type Service(handlerLog, resolve) =
    
    let (|CategoryId|) (id: string) = Equinox.CatId("Aggregate", id)
    
    /// Maps a ClientId to Handler for the relevant stream
    let (|Stream|) (CategoryId catId) = Handler(handlerLog, resolve catId)

    let render (s: Folds.State) : View =
        {   sorted = s.happened }

    /// Read the present state
    // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
    member __.Read(Stream stream) : Async<View> =
        stream.Query (fun s -> render s)

    /// Execute the specified command 
    member __.Execute(Stream stream, command) : Async<unit> =
        stream.Execute command