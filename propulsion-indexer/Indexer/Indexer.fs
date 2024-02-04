module IndexerTemplate.Indexer.Indexer

type Outcome = Metrics.Outcome

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval, verboseStore, abendThreshold) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval, abendThreshold = abendThreshold)

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
        Equinox.CosmosStore.Core.Log.InternalMetrics.dump Serilog.Log.Logger

    override _.Classify(e) =
        match e with
        | OutcomeKind.StoreExceptions kind -> kind
        | Equinox.CosmosStore.Exceptions.ServiceUnavailable when not verboseStore -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

open IndexerTemplate.Domain

let sourceCategories = Todo.Reactions.categories

let toSummaryEventData (x: Todo.Fold.State): TodoIndex.Events.SummaryData =
    { items =
        [| for x in x.items ->
            { id = x.id; order = x.order; title = x.title; completed = x.completed } |] }

let handle (sourceService: Todo.Service) (summaryService: TodoIndex.Service) stream events = async {
    match struct (stream, events) with
    | Todo.Reactions.ImpliesStateChange clientId  ->
        let! version', summary = sourceService.QueryWithVersion(clientId, toSummaryEventData)
        match! summaryService.TryIngest(clientId, version', summary) with
        | true -> return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Ok (1, events.Length - 1)
        | false -> return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Skipped events.Length
    | _ -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.NotApplicable events.Length }

module Factory =

    let create store =
        let srcService = Todo.Factory.create store
        let dstService = TodoIndex.Factory.create store
        Some sourceCategories, handle srcService dstService
