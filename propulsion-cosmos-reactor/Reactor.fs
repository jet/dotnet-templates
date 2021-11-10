module ReactorTemplate.Reactor

[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used : int * unused : int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count : int
    /// Handler determined the events were not relevant to its duties and performed no actions
    /// e.g. wrong category, events that dont imply a state change
    | NotApplicable of count : int

/// Gathers stats based on the outcome of each Span processed for emission, at intervals controlled by `StreamsConsumer`
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, skipped, na = 0, 0, 0

    override _.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0

// map from external contract to internal contract defined by the aggregate
let toSummaryEventData ( x : Contract.SummaryInfo) : TodoSummary.Events.SummaryData =
    { items =
        [| for x in x.items ->
            { id = x.id; order = x.order; title = x.title; completed = x.completed } |] }

let handle
        (sourceService : Todo.Service)
        (summaryService : TodoSummary.Service)
        (stream, span : Propulsion.Streams.StreamSpan<_>) = async {
    match stream, span with
    | Todo.Reactions.Parse (clientId, events) when events |> Seq.exists Todo.Reactions.impliesStateChange ->
        let! version', summary = sourceService.QueryWithVersion(clientId, Contract.ofState)
        match! summaryService.TryIngest(clientId, version', toSummaryEventData summary) with
        | true -> return Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Ok (1, span.events.Length - 1)
        | false -> return Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Skipped span.events.Length
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.events.Length }
