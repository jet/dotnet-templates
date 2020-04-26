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

/// Gathers stats based on the outcome of each Span processed for emission, at intervals controlled by `StreamsConsumer`
type Stats(log, statsInterval, stateInterval, ?logExternalStats) =
#if kafkaEventSpans
    inherit Propulsion.Kafka.StreamsConsumerStats<Outcome>(log, statsInterval, stateInterval)
#else
#if blank
    inherit Propulsion.Streams.Projector.Stats<Outcome>(log, statsInterval, stateInterval)
#else
    inherit Propulsion.Streams.Sync.Stats<Outcome>(log, statsInterval, stateInterval)
#endif
#endif

    let mutable ok, skipped, na = 0, 0, 0

    override __.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
    override __.HandleExn exn =
        log.Information(exn, "Unhandled")

    override __.DumpStats () =
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0
        logExternalStats |> Option.iter (fun dumpTo -> dumpTo log)

let generate stream version summary =
    let event = Contract.encode summary
    Propulsion.Codec.NewtonsoftJson.RenderedSummary.ofStreamEvent stream version event

#if blank
let handle
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<_>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) = async {
    match stream, span with
    | Contract.Input.Match (clientId, events) ->
        for version, event in events do
            let summary =
                match event with
                | Contract.Input.EventA { field = x } -> Contract.EventA { value = x }
                | Contract.Input.EventB { field = x } -> Contract.EventB { value = x }
            let wrapped = generate stream version summary
            let! _ = produceSummary wrapped in ()
        return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Ok (events.Length, 0)
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.events.Length }
#else
let handle
        (service : Todo.Service)
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<_>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) = async {
    match stream, span with
    | Todo.Events.Match (clientId, events) ->
        if events |> Seq.exists Todo.Fold.impliesStateChange then
            let! version', summary = service.QueryWithVersion(clientId, Contract.ofState)
            let wrapped = generate stream version' (Contract.Summary summary)
            let! _ = produceSummary wrapped
            return Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Ok (1, events.Length - 1)
        else
            return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Skipped events.Length
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.events.Length }
#endif
//#endif