[<AutoOpen>]
module Patterns.Domain.Infrastructure

/// Buffers events accumulated from a series of decisions while evolving the presented `state` to reflect said proposed `Events`
type Accumulator<'e, 's>(originState : 's, fold : 's -> seq<'e> -> 's) =
    let mutable state = originState
    let pendingEvents = ResizeArray<'e>()
    let (|Apply|) (xs : #seq<'e>) = state <- fold state xs; pendingEvents.AddRange xs

    /// Run an Async interpret function that does not yield a result
    member _.Transact(interpret : 's -> Async<#seq<'e>>) : Async<unit> = async {
        let! Apply = interpret state in return () }

    /// Run an Async decision function, buffering and applying any Events yielded
    member _.Transact(decide : 's -> Async<'r * #seq<'e>>) : Async<'r> = async {
        let! r, Apply = decide state in return r }

    /// Run a decision function, buffering and applying any Events yielded
    member _.Transact(decide : 's -> 'r * #seq<'e>) : 'r =
        let r, Apply = decide state in r

    /// Accumulated events based on the Decisions applied to date
    member _.Events : 'e list =
        List.ofSeq pendingEvents

//    /// Run a decision function that does not yield a result
//    member x.Transact(interpret) : unit =
//        x.Transact(fun state -> (), interpret state)

//    /// Projects from the present state including accumulated events
//    member _.Query(render : 's -> 'r) : 'r =
//        render state

/// For particularly common patterns used within a given app, sometimes it can make sense to name the pattern locally
/// There are definitely trade-offs to this - one person's great intention revealing name is another's layer of obfuscation
type Equinox.Decider<'e, 's> with

     member x.Transact(decide, mapResult) =
        x.TransactEx((fun c -> async { return! decide c.State }), fun r _c -> mapResult r)

     // in baseline V4 API Decider API
     member x.Transact(decide) =
        x.TransactAsync decide
