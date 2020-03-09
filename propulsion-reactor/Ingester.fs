module ReactorTemplate.Ingester

open System

[<RequireQualifiedAccess>]
type Outcome = NoRelevantEvents of count : int | Ok of count : int | Skipped of count : int

/// Gathers stats based on the outcome of each Span processed for emission, at intervals controlled by `StreamsConsumer`
type Stats(log, ?statsInterval, ?stateInterval) =
#if (!kafkaEventSpans)
    inherit Propulsion.Streams.Projector.Stats<Outcome>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))
#else
    inherit Propulsion.Kafka.StreamsConsumerStats<int64*Outcome>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))
#endif
    let mutable ok, skipped, na = 0, 0, 0

#if (!kafkaEventSpans)
    override __.HandleOk res = res |> function
        | Outcome.Ok count -> ok <- ok + 1; skipped <- skipped + count - 1
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NoRelevantEvents count -> na <- na + count
#else
    override __.HandleOk res = res |> function
        | _, Outcome.Ok count -> ok <- ok + 1; skipped <- skipped + count - 1
        | _, Outcome.Skipped count -> skipped <- skipped + count
        | _, Outcome.NoRelevantEvents count -> na <- na + count
#endif
    override __.DumpStats () =
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0

#if blank
let tryHandle (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64 option*Outcome> = async {
    match stream, span with
    | FsCodec.StreamName.CategoryAndId ("Todos",id), _ ->
        let ok, version' = true, None
        // "TODO: add handler code"
        match ok with
        | true -> return version', Outcome.Ok span.events.Length
        | false -> return version', Outcome.Skipped span.events.Length
    | _ -> return None, Outcome.NoRelevantEvents span.events.Length }
#else
// map from external contract to internal contract defined by the aggregate
let toSummaryEventData ( x : Contract.SummaryInfo) : TodoSummary.Events.SummaryData =
    { items =
        [| for x in x.items ->
            { id = x.id; order = x.order; title = x.title; completed = x.completed } |]}

let tryHandle
        (sourceService : Todo.Service)
        (summaryService : TodoSummary.Service)
        (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64 option*Outcome> = async {
    match stream, span with
    | Todo.Events.Match (clientId, events) when events |> Seq.exists Todo.Fold.impliesStateChange ->
        let! version', summary = sourceService.QueryWithVersion(clientId, Contract.ofState)
        match! summaryService.Ingest(clientId, version', toSummaryEventData summary) with
        | true -> return Some version', Outcome.Ok span.events.Length
        | false -> return Some version', Outcome.Skipped span.events.Length
    | _ -> return None, Outcome.NoRelevantEvents span.events.Length }
#endif

let handleStreamEvents tryHandle (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64*Outcome> = async {
    match! tryHandle (stream, span) with
    // We need to yield the next write position, which will be after the version we've just generated the summary based on
    | Some version', outcome -> return version'+1L, outcome
    // If we're ignoring the events, we mark the next write position to be one beyond the last one offered
    | _, outcome -> return span.index + span.events.LongLength, outcome }
