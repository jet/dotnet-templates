module ReactorTemplate.Reactor

type Outcome = Metrics.Outcome

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, skipped, na = 0, 0, 0

    override _.HandleOk res =
        Metrics.observeReactorOutcome res
        match res with
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

let reactionCategories = Todo.Reactions.categories

let handle (sourceService : Todo.Service) (summaryService : TodoSummary.Service) stream span = async {
    match stream, span with
    | Todo.Reactions.Parse (clientId, events) when events |> Seq.exists Todo.Reactions.impliesStateChange ->
        let! version', summary = sourceService.QueryWithVersion(clientId, Contract.ofState)
        match! summaryService.TryIngest(clientId, version', toSummaryEventData summary) with
        | true -> return struct (Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Ok (1, span.Length - 1))
        | false -> return Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Skipped span.Length
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.Length }

module Config =

    let createHandler store =
        let srcService = Todo.Config.create store
        let dstService = TodoSummary.Config.create store
        handle srcService dstService

type Config private () =
    
    static member StartSink(log : Serilog.ILogger, stats : Stats, maxConcurrentStreams : int, handle : _ -> _ -> Async<_>, maxReadAhead : int) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval.Period)
