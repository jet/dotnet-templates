module ProjectorTemplate.Handler
#if cosmos

#if     parallelOnly
let mapToStreamItems : seq<'a> -> IReadOnlyList<'a> = upcast
#else // cosmos && !parallelOnly
//let replaceLongDataWithNull (x : FsCodec.ITimelineEvent<byte[]>) : FsCodec.ITimelineEvent<_> =
//    if x.Data.Length < 900_000 then x
//    else FsCodec.Core.TimelineEvent.Create(x.Index, x.EventType, null, x.Meta, timestamp=x.Timestamp)
//
//let hackDropBigBodies (e : Propulsion.Streams.StreamEvent<_>) : Propulsion.Streams.StreamEvent<_> =
//    { stream = e.stream; event = replaceLongDataWithNull e.event }

let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
    docs
    |> Seq.collect  Propulsion.Cosmos.EquinoxCosmosParser.enumStreamEvents
    // TODO use Seq.filter and/or Seq.map to adjust what's being sent etc
    // |> Seq.map hackDropBigBodies
#endif // !parallelOnly
#else // !cosmos
open Propulsion.EventStore

/// Responsible for inspecting and then either dropping or tweaking events coming from EventStore
// NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
let tryMapEvent filterByStreamName (x : EventStore.ClientAPI.ResolvedEvent) =
    match x.Event with
    | e when not e.IsJson || e.EventStreamId.StartsWith "$" || not (filterByStreamName e.EventStreamId) -> None
    | PropulsionStreamEvent e -> Some e

#endif // !cosmos

#if kafka
#if     (cosmos && parallelOnly) // kafka && cosmos && parallelOnly
type ExampleOutput = { Id : string }

let render (doc : Microsoft.Azure.Documents.Document) : string * string =
    let equinoxPartition, documentId = doc.GetPropertyValue "p", doc.Id
    equinoxPartition, FsCodec.NewtonsoftJson.Serdes.Serialize { Id = documentId }
#else // kafka && !(cosmos && parallelOnly)
// Each outcome from `handle` is passed to `HandleOk` or `HandleExn` by the scheduler, DumpStats is called at `statsInterval`
// The incoming calls are all sequential - the logic does not need to consider concurrent incoming calls
type ProductionStats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Sync.Stats<unit>(log, statsInterval, stateInterval)

    // TODO consider whether it's warranted to log every time a message is produced given the stats will periodically emit counts
    override __.HandleOk(()) = log.Warning("Produced")
    // TODO consider whether to log cause of every individual produce failure in full (Failure counts are emitted periodically)
    override __.HandleExn exn = log.Information(exn, "Unhandled")

/// Responsible for wrapping a span of events for a specific stream into an envelope
/// The well-defined Propulsion.Codec `RenderedSpan` represents the accumulated span of events for a given stream as an
///   array within each message in order to maximize throughput within constraints Kafka's model implies (we are aiming
///   to preserve ordering at stream (key) level for messages produced to the topic)
// TODO NOTE: The bulk of any manipulation should take place before events enter the scheduler, i.e. in program.fs
// TODO NOTE: While filtering out entire categories is appropriate, you should not filter within a given stream (i.e., by event type)
let render (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
    let value =
        span
        |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
        |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
    return FsCodec.StreamName.toString stream, value }
#endif // kafka && !(cosmos && parallelOnly)
#else // !kafka
// Each outcome from `handle` is passed to `HandleOk` or `HandleExn` by the scheduler, DumpStats is called at `statsInterval`
// The incoming calls are all sequential - the logic does not need to consider concurrent incoming calls
type ProjectorStats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Projector.Stats<int>(log, statsInterval, stateInterval)

    let mutable totalCount = 0

    // TODO consider best balance between logging or gathering summary information per handler invocation
    // here we don't log per invocation (such high level stats are already gathered and emitted) but accumulate for periodic emission
    override __.HandleOk count =
        totalCount <- totalCount + count
    // TODO consider whether to log cause of every individual failure in full (Failure counts are emitted periodically)
    override __.HandleExn exn =
        log.Information(exn, "Unhandled")

    override __.DumpStats() =
        log.Information(" Total events processed {total}", totalCount)
        totalCount <- 0

let handle (_stream, span: Propulsion.Streams.StreamSpan<_>) = async {
    let r = System.Random()
    let ms = r.Next(1, span.events.Length)
    do! Async.Sleep ms
    return Propulsion.Streams.SpanResult.AllProcessed, span.events.Length }
#endif // !kafka