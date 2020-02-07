module ProjectorTemplate.Handler

open Propulsion.EventStore
open Serilog

//#if kafka
/// Responsible for inspecting and then either dropping or tweaking events coming from EventStore
// NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
let tryMapEvent filterByStreamName (x : EventStore.ClientAPI.ResolvedEvent) =
    match x.Event with
    | e when not e.IsJson || e.EventStreamId.StartsWith "$" || not (filterByStreamName e.EventStreamId) -> None
    | PropulsionStreamEvent e -> Some e

/// Responsible for wrapping a span of events for a specific stream into an envelope (we use the well-known Propulsion.Codec form)
/// Most manipulation should take place before events enter the scheduler
let render (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
    let value =
        span
        |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
        |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
    return FsCodec.StreamName.toString stream, value }
//#else
let handle (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
    // TODO add Outcomes, stats
    Log.Information("Handled {stream} up to {index}", stream, span.index)
}
//#endif