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
    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0

    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

// map from external contract to internal contract defined by the aggregate
let toSummaryEventData (x: Contract.SummaryInfo): TodoSummary.Events.SummaryData =
    { items =
        [| for x in x.items ->
            { id = x.id; order = x.order; title = x.title; completed = x.completed } |] }

let reactionCategories = Todo.Reactions.categories

let handle (sourceService: Todo.Service) (summaryService: TodoSummary.Service) stream events = async {
    match struct (stream, events) with
    | Todo.Reactions.ImpliesStateChange clientId  ->
        let! version', summary = sourceService.QueryWithVersion(clientId, Contract.ofState)
        match! summaryService.TryIngest(clientId, version', toSummaryEventData summary) with
        | true -> return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Ok (1, events.Length - 1)
        | false -> return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Skipped events.Length
    | _ -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.NotApplicable events.Length }

module Factory =

    let createHandler store =
        let srcService = Todo.Factory.create store
        let dstService = TodoSummary.Factory.create store
        handle srcService dstService

type Factory private () =
    
    static member StartSink(log, stats, maxConcurrentStreams, handle, maxReadAhead) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats)
