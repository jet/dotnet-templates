module ReactorTemplate.Ingester

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
type Stats(log, statsInterval, stateInterval, verboseStore, ?logExternalStats) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, skipped, na = 0, 0, 0

    override _.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
    override _.Classify(exn) =
        match exn with
        | Equinox.DynamoStore.Exceptions.ProvisionedThroughputExceeded -> Propulsion.Streams.OutcomeKind.RateLimited
        | :? Microsoft.Azure.Cosmos.CosmosException as e
            when (e.StatusCode = System.Net.HttpStatusCode.TooManyRequests
                  || e.StatusCode = System.Net.HttpStatusCode.ServiceUnavailable)
                 && not verboseStore -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0
        logExternalStats |> Option.iter (fun dumpTo -> dumpTo log)

#if blank
let reactionCategories = [| "Todos" |]
    
let handle stream (span : Propulsion.Streams.StreamSpan<_>) = async {
    match stream, span with
    | FsCodec.StreamName.CategoryAndId ("Todos", id), _ ->
        let ok = true
        // "TODO: add handler code"
        match ok with
        | true -> return struct (Propulsion.Streams.SpanResult.AllProcessed, Outcome.Ok (1, span.Length - 1))
        | false -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Skipped span.Length
    | _ -> return Propulsion.Streams.AllProcessed, Outcome.NotApplicable span.Length }
#else
// map from external contract to internal contract defined by the aggregate
let toSummaryEventData ( x : Contract.SummaryInfo) : TodoSummary.Events.SummaryData =
    { items =
        [| for x in x.items ->
            { id = x.id; order = x.order; title = x.title; completed = x.completed } |] }

let reactionCategories = [| Todo.Reactions.Category |]

let handle (sourceService : Todo.Service) (summaryService : TodoSummary.Service) stream span = async {
    match stream, span with
    | Todo.Reactions.Parse (clientId, events) when events |> Seq.exists Todo.Reactions.impliesStateChange ->
        let! version', summary = sourceService.QueryWithVersion(clientId, Contract.ofState)
        match! summaryService.TryIngest(clientId, version', toSummaryEventData summary) with
        | true -> return struct (Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Ok (1, span.Length - 1))
        | false -> return Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Skipped span.Length
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.Length }
#endif

type Config private () =
    
    static member StartSink(log : Serilog.ILogger, stats : Propulsion.Streams.Scheduling.Stats<_, _>,
                            handle : FsCodec.StreamName -> Propulsion.Streams.Default.StreamSpan -> Async<struct (Propulsion.Streams.SpanResult * Outcome)>,
                            maxReadAhead : int, maxConcurrentStreams : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval.Period,
                                                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
    
    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Config.log) sink reactionCategories sourceConfig
