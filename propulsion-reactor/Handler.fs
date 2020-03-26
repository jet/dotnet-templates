module ReactorTemplate.Handler
//#if multiSource

open Propulsion.EventStore

/// Responsible for inspecting and then either dropping or tweaking events coming from EventStore
// NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
let tryMapEvent filterByStreamName (x : EventStore.ClientAPI.ResolvedEvent) =
    match x.Event with
    | e when not e.IsJson || e.EventStreamId.StartsWith "$" || not (filterByStreamName e.EventStreamId) -> None
    | PropulsionStreamEvent e -> Some e
//#endif
//#if kafka

[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used : int * unused : int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count : int
    /// Handler determined the events were not relevant to its duties and performed no decoding or processing
    | NotApplicable of count : int

let mins x = System.TimeSpan.FromMinutes x

/// Gathers stats based on the outcome of each Span processed for emission, at intervals controlled by `StreamsConsumer`
type Stats(log, statsInterval, stateInterval, ?logExternalStats) =
#if (!kafkaEventSpans)
    inherit Propulsion.Streams.Sync.StreamsSyncStats<Outcome>(log, statsInterval, stateInterval)
#else
    inherit Propulsion.Kafka.StreamsConsumerStats<int64 * Outcome>(log, statsInterval, stateInterval)
#endif

    let mutable ok, skipped, na = 0, 0, 0

#if (!kafkaEventSpans)
    override __.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
#else
    override __.HandleOk res = res |> function
        | _, Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | _, Outcome.Skipped count -> skipped <- skipped + count
        | _, Outcome.NotApplicable count -> na <- na + count
#endif
    override __.DumpStats () =
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0
        logExternalStats |> Option.iter log

let generate stream version info =
    let event = Contract.codec.Encode(None, Contract.Summary info)
    Propulsion.Codec.NewtonsoftJson.RenderedSummary.ofStreamEvent stream version event

let tryHandle
        (service : Todo.Service)
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<_>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64 option * Outcome> = async {
    match stream, span with
    | Todo.Events.Match (clientId, events) ->
        if events |> Seq.exists Todo.Fold.impliesStateChange then
            let! version', summary = service.QueryWithVersion(clientId, Contract.ofState)
            let wrapped = generate stream version' summary
            let! _ = produceSummary wrapped
            return Some version', Outcome.Ok (1, events.Length - 1)
        else
            return None, Outcome.Skipped events.Length
    | _ -> return None, Outcome.NotApplicable span.events.Length }

let handleStreamEvents tryHandle (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64 * Outcome> = async {
    match! tryHandle (stream, span) with
    // We need to yield the next write position, which will be after the version we've just generated the summary based on
    | Some version', outcome -> return version' + 1L, outcome
    // If we're ignoring the events, we mark the next write position to be one beyond the last one offered
    | _, outcome -> return span.index + span.events.LongLength, outcome }
//#endif