[<AutoOpen>]
module Patterns.Domain.Infrastructure

/// Buffers events accumulated from a series of decisions while evolving the presented `state` to reflect said proposed `Events`
type Accumulator<'event, 'state>(originState, fold : 'state -> 'event seq -> 'state) =
    let pendingEvents = ResizeArray()
    let mutable state = originState

    let apply (events : 'event seq) =
        pendingEvents.AddRange events
        state <- fold state events

    /// Run a decision function, buffering and applying any Events yielded
    member _.Transact decide =
        let r, events = decide state
        apply events
        r

    /// Run a decision function that does not yield a result
//    member x.Transact decide =
//        x.Transact(fun state -> (), decide state)

    /// Run an Async decision function, buffering and applying any Events yielded
    member _.TransactAsync decide = async {
        let! r, events = decide state
        apply events
        return r }

    /// Run an Async decision function that does not yield a result
    member x.TransactAsync decide = async {
        let! events = decide state
        apply events }

//    /// Projects from the present state including accumulated events
//    member _.Query f = f state

    /// Accumulated events based on the Decisions applied to date
    member _.Events = List.ofSeq pendingEvents
