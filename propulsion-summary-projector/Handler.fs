module ProjectorTemplate.Handler

let (|ClientId|) = ClientId.parse

let (|Decode|) (codec : FsCodec.IUnionEncoder<_, _, _>) stream (span : Propulsion.Streams.StreamSpan<_>) =
    span.events |> Seq.choose (EventCodec.tryDecode codec Serilog.Log.Logger stream)

let tryHandle
        (service : Todo.Service)
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<_>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64 option> = async {
    match stream, span with
    | Category (Todo.Events.category, ClientId clientId), (Decode Todo.Events.codec stream events)
            when events |> Seq.exists Todo.Fold.impliesStateChange ->
        let! version', summary = service.QueryWithVersion(clientId, Producer.Contract.ofState)
        let wrapped = Producer.generate stream version' summary
        let! _ = produceSummary wrapped
        return Some version'
    | _ -> return None }

let handleStreamEvents (service, produceSummary) (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64> = async {
    match! tryHandle service produceSummary (stream, span) with
    // We need to yield the next write position, which will be after the version we've just generated the summary based on
    | Some version' -> return version'+1L
    // If we're ignoring the events, we mark the next write position to be one beyond the last one offered
    | _ -> return (Array.last span.events).Index+1L }

let handleCosmosStreamEvents = handleStreamEvents
let handleEventStoreStreamEvents = handleStreamEvents