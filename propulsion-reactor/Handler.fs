module ReactorTemplate.Handler
//#if (!noEventStore)

open Propulsion.EventStore

/// Responsible for inspecting and then either dropping or tweaking events coming from EventStore
// NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
let tryMapEvent filterByStreamName (x : EventStore.ClientAPI.ResolvedEvent) =
    match x.Event with
    | e when not e.IsJson || e.EventStreamId.StartsWith "$" || not (filterByStreamName e.EventStreamId) -> None
    | PropulsionStreamEvent e -> Some e
//#endif
//#if kafka

//#if !raw
let generate stream version info =
    let event = Contract.codec.Encode(None, Contract.Summary info)
    Propulsion.Codec.NewtonsoftJson.RenderedSummary.ofStreamEvent stream version event

let tryHandle
        (service : Todo.Service)
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<_>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64 option> = async {
    match stream, span with
    | Todo.Events.Match (clientId, events) when events |> Seq.exists Todo.Fold.impliesStateChange ->
        let! version', summary = service.QueryWithVersion(clientId, Contract.ofState)
        let wrapped = generate stream version' summary
        let! _ = produceSummary wrapped
        return Some version'
    | _ -> return None }

let handleStreamEvents (service, produceSummary) (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64> = async {
    match! tryHandle service produceSummary (stream, span) with
    // We need to yield the next write position, which will be after the version we've just generated the summary based on
    | Some version' -> return version'+1L
    // If we're ignoring the events, we mark the next write position to be one beyond the last one offered
    | _ -> return span.index + span.events.LongLength }
//#else
/// Responsible for wrapping a span of events for a specific stream into an envelope (we use the well-known Propulsion.Codec form)
/// Most manipulation should take place before events enter the scheduler
let render (stream : FsCodec.StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
    let value =
        span
        |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
        |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
    return FsCodec.StreamName.toString stream, value }
//#endif
//#endif