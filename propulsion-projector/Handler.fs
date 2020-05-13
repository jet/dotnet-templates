module ProjectorTemplate.Handler

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

#if kafka
#if parallelOnly
type ExampleOutput = { Id : string }

let render (doc : Microsoft.Azure.Documents.Document) : string * string =
    let equinoxPartition, documentId = doc.GetPropertyValue "p", doc.Id
    equinoxPartition, FsCodec.NewtonsoftJson.Serdes.Serialize { Id = documentId }
#else
/// Responsible for wrapping a span of events for a specific stream into an envelope
/// The well-defined Propulsion.Codec form represents the accumulated span of events for a given stream as an array within
///   each message in order to maximize throughput within constraints Kafka's model implies (we are aiming to preserve
///   ordering at stream (key) level for messages produced to the topic)
// TODO NOTE: The bulk of any manipulation should take place before events enter the scheduler, i.e. in program.fs
// TODO NOTE: While filtering out entire categories is appropriate, you should not filter within a given stream (i.e., by event type)
let render (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
    let value =
        span
        |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
        |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
    return FsCodec.StreamName.toString stream, value }
#endif
#else
let handle (_stream, span: Propulsion.Streams.StreamSpan<_>) = async {
    let r = System.Random()
    let ms = r.Next(1, span.events.Length)
    do! Async.Sleep ms
    return Propulsion.Streams.SpanResult.AllProcessed, span.events.Length }
#endif