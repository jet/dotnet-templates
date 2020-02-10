module ProjectorTemplate.Handler

open Propulsion.Cosmos

//let replaceLongDataWithNull (x : FsCodec.ITimelineEvent<byte[]>) : FsCodec.ITimelineEvent<_> =
//    if x.Data.Length < 900_000 then x
//    else FsCodec.Core.TimelineEvent.Create(x.Index, x.EventType, null, x.Meta, timestamp=x.Timestamp)
//
//let hackDropBigBodies (e : Propulsion.Streams.StreamEvent<_>) : Propulsion.Streams.StreamEvent<_> =
//    { stream = e.stream; event = replaceLongDataWithNull e.event }

let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
    docs
    |> Seq.collect EquinoxCosmosParser.enumStreamEvents
    // TODO use Seq.filter and/or Seq.map to adjust what's being sent etc
    // |> Seq.map hackDropBigBodies

#if kafka
#if parallelOnly
type ExampleOutput = { Id : string }

let render (doc : Microsoft.Azure.Documents.Document) : string * string =
    let equinoxPartition, documentId = doc.GetPropertyValue "p", doc.Id
    equinoxPartition, FsCodec.NewtonsoftJson.Serdes.Serialize { Id = documentId }
#else
let render (stream: FsCodec.StreamName, span: Propulsion.Streams.StreamSpan<_>) = async {
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
    do! Async.Sleep ms }
#endif