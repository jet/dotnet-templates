module PrunerTemplate.Handler

open System

// As we're not looking at the bodies of the events in the course of the shouldPrune decision, we remove them
//   from the Event immediately in order to avoid consuming lots of memory without purpose while they're queued
let removeDataAndMeta (x : FsCodec.ITimelineEvent<byte[]>) : FsCodec.ITimelineEvent<_> =
    FsCodec.Core.TimelineEvent.Create(x.Index, x.EventType, null, timestamp=x.Timestamp)

// We prune events from the Primary Container as we reach the point where there's no benefit to them staying there. e.g.
// 1. If a ChangeFeedProcessor (including new ones) needs to be able to walk those events
// 2. If transactional processing will benefit from being able to load the events using the provisioned capacity on the Primary
// 3. All relevant systems are configured to be able to fall back to the Secondary where the head of a stream being read has been pruned
// NOTE - DANGEROUS - events submitted to the CosmosPruner get removed from the supplied Context!
let shouldPrune category (age : TimeSpan) =
    match category, age.TotalDays with
    // TODO define pruning criteria
    | "CategoryName",  age -> age > 30.
    | _ -> false

// Only relevant (copied to secondary container, meeting expiration criteria) events get fed into the CosmosPruner for removal
// NOTE - DANGEROUS - events submitted to the CosmosPruner get removed from the supplied Context!
let selectPrunable (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    let asOf = DateTimeOffset.UtcNow
    for se in Propulsion.Cosmos.EquinoxCosmosParser.enumStreamEvents changeFeedDocument do
        let (FsCodec.StreamName.CategoryAndId (cat,_)) = se.stream
        let age = asOf - se.event.Timestamp
        if shouldPrune cat age then
            yield { se with event = removeDataAndMeta se.event }
}
