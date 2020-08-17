module PrunerTemplate.Handler

open System

type Outcome =
    | Ok of completed : int * deferred : int
    | Nop of int

/// Facilitates periodically emitting a summary of the activities being carried out in this projector
type ProjectorStats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Projector.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable nops, totalRedundant, ops, totalDeletes, totalDeferred = 0, 0, 0, 0, 0

    override __.HandleOk outcome =
        match outcome with
        | Nop count ->
            nops <- nops + 1
            totalRedundant <- totalRedundant + count
        | Ok (completed, deferred) ->
            ops <- ops + 1
            totalDeletes <- totalDeletes + completed
            totalDeferred <- totalDeferred + deferred

    override __.HandleExn exn =
        log.Information(exn, "Unhandled")

    override __.DumpStats() =
        log.Information(" Deleted {ops}r {deletedCount}e Deferred {deferred}e Redundant {nops}r {nopCount}e",
                        ops, totalDeletes, nops, totalDeferred, totalRedundant)
        ops <- 0; totalDeletes <- 0; nops <- 0; totalDeferred <- totalDeferred; totalRedundant <- 0

// As we're not looking at the bodies of the events in the course of the shouldPrune or handle code, we remove them
//   from the Event immediately in order to avoid consuming lots of memory without purpose while they're queued
let removeDataAndMeta (x : FsCodec.ITimelineEvent<byte[]>) : FsCodec.ITimelineEvent<_> =
    FsCodec.Core.TimelineEvent.Create(x.Index, x.EventType, null, null, timestamp=x.Timestamp)

// We prune events from the Primary Container as we reach the point where there's no benefit to them staying there. e.g.
// 1. If a ChangeFeedProcessor (including new ones) needs to be able to walk those events
// 2. If transactional processing will benefit from being able to load the events using the provisioned capacity on the Primary
// 3. All relevant systems are configured to be able to fall back to the Secondary where the head of a stream being read has been pruned
let shouldPrune category (age : TimeSpan) =
    match category, age.TotalDays with
    | "LokiPickTicketReservations",  age -> age > 30.
    | "LokiDcBatch",                 age -> age > 6. * 30.
    | "LokiDcTransmissions",         age -> age > 7.
    | _ -> false

// Only relevant (copied to secondary container, meeting expiration criteria) events get fed into the Projector for consideration
let selectExpired (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    let asOf = DateTimeOffset.UtcNow
    for se in Propulsion.Cosmos.EquinoxCosmosParser.enumStreamEvents changeFeedDocument do
        let (FsCodec.StreamName.CategoryAndId (cat,_)) = se.stream
        let age = asOf - se.event.Timestamp
        if shouldPrune cat age then
            yield { se with event = removeDataAndMeta se.event }
}

// Per set of accumulated events per stream (selected via `selectExpired`), attempt to prune up to the high water mark
let handle tryTrimBefore (stream, span: Propulsion.Streams.StreamSpan<_>) = async {
    // The newest event eligible for deletion defines the cutoff point
    let beforeIndex = (Array.last span.events).Index + 1L
    // Depending on the way the events are batched, requests break into three groupings:
    // 1. All requested events already deleted, no writes took place
    //    (if trimmedPos is beyond requested Index, Propulsion will discard the requests via the OverrideWritePosition)
    // 2. All events deleted as requested
    //    (N events over M batches were removed)
    // 3. Some deletions deferred
    //    (requested trim point was in the middle of a batch; touching it would put the batch out of order)
    //    in this case, we mark the event as handled and await a successor event triggering another attempt
    let deleted, deferred, trimmedPos = tryTrimBefore stream beforeIndex
    // Categorize the outcome so the stats handler can summarize the work being carried out
    let res = if deleted = 0 && deferred = 0 then Nop span.events.Length else Ok (deleted, deferred)
    // For case where we discover events have already been deleted beyond our requested position, signal to reader to drop events
    let writePos = max trimmedPos beforeIndex
    return Propulsion.Streams.SpanResult.OverrideWritePosition writePos, res
}
