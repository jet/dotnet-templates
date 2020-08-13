module PrunerTemplate.Handler

open System

type ProjectorStats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Projector.Stats<int>(log, statsInterval, stateInterval)

    let mutable totalCount = 0

    override __.HandleOk count =
        totalCount <- totalCount + count
    override __.HandleExn exn =
        log.Information(exn, "Unhandled")

    override __.DumpStats() =
        log.Information(" Total events processed {total}", totalCount)
        totalCount <- 0

// As we're not looking at the bodies of the events in the course of the shouldPrune or handle code, we remove that
//   from the event immediately in order that we're not consuming lots of memory without purpose
let removeDataAndMeta (x : FsCodec.ITimelineEvent<byte[]>) : FsCodec.ITimelineEvent<_> =
    FsCodec.Core.TimelineEvent.Create(x.Index, x.EventType, null, null, timestamp=x.Timestamp)

// We prune events from the Primary Container as we reach the point where there's no benefit to them staying there. e.g.
// 1. If a ChangeFeedProcessor (including new ones) needs to be able to walk those events
// 2. If transactional processing will benefit from being able to load the events using the provisioned capacity on the Primary
// 3. All relevant systems are configured to be able to fall back to the Secondary where the head of a stream being read has been pruned
let shouldPrune (asOf : DateTimeOffset) category timestamp =
    match category, (asOf - timestamp).TotalDays with
    | "LokiPickTicketReservations", age -> age > 30
    | "LokiDcBatch", age when age > 6*30 -> true
    | "LokiDcTransmissions", age when age > 7 -> true
    | _ -> false

// Only relevant events get fed into the Projector for consideration
let selectExpired (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    let asOf = DateTimeOffset.UtcNow
    for se in Propulsion.Cosmos.EquinoxCosmosParser.enumStreamEvents changeFeedDocument do
        let (FsCodec.StreamName.CategoryAndId (cat,_)) = se.stream
        if shouldPrune asOf cat se.event then
            yield { se with removeDataAndMeta event }
}
ƒƒ
// search backwards to identify first item to delete
// 1. If delete indicates that we have deleted further than we are asking, move write position to there
// if delete fails (only represents portion of a batch), ideally we elide event bodies but remember fact it is to be deleted

// Phase 1: Keep snapshots only in primary (eventually be able to read from secondary?)


// Cycle
// 1. reset offsets in aux
// 2. CFP walk determining id to delete <=
// 2a. if delete says done, update position
// 2b. if delete says can't do in full, update position

// Each outcome from `handle` is passed to `HandleOk` or `HandleExn` by the scheduler, DumpStats is called at `statsInterval`
// The incoming calls are all sequential - the logic does not need to consider concurrent incoming calls

let handle (_stream, span: Propulsion.Streams.StreamSpan<_>) = async {
    let r = System.Random()
    let ms = r.Next(1, span.events.Length)
    do! Async.Sleep ms
    return Propulsion.Streams.SpanResult.AllProcessed, span.events.Length }
