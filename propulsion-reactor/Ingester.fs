module ReactorTemplate.Ingester

[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used: int * unused: int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count: int
    /// Handler determined the events were not relevant to its duties and performed no actions
    /// e.g. wrong category, events that dont imply a state change
    | NotApplicable of count: int

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval, verboseStore, ?logExternalStats) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, skipped, na = 0, 0, 0
    override _.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0
        logExternalStats |> Option.iter (fun dumpTo -> dumpTo log)

    override _.Classify(exn) =
        match exn with
        | OutcomeKind.StoreExceptions kind -> kind 
        | Equinox_CosmosStore_Exceptions.ServiceUnavailable when not verboseStore -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

#if blank
let [<Literal>] Category = "Todos"
let reactionCategories = [| Category |]
    
let handle stream (events: Propulsion.Sinks.Event[]) = async {
    match stream, events with
    | FsCodec.StreamName.CategoryAndId (Category, id), _ ->
        let ok = true
        // "TODO: add handler code"
        match ok with
        | true -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.Ok (1, events.Length - 1)
        | false -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.Skipped events.Length
    | _ -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.NotApplicable events.Length }
#else
// map from external contract to internal contract defined by the aggregate
let toSummaryEventData (x: Contract.SummaryInfo): TodoSummary.Events.SummaryData =
    { items =
        [| for x in x.items ->
            { id = x.id; order = x.order; title = x.title; completed = x.completed } |] }

let reactionCategories = Todo.Reactions.categories

let handle (sourceService: Todo.Service) (summaryService: TodoSummary.Service) stream events = async {
    match struct (stream, events) with
    | Todo.Reactions.ImpliesStateChange (clientId, eventCount) ->
        let! version', summary = sourceService.QueryWithVersion(clientId, Contract.ofState)
        match! summaryService.TryIngest(clientId, version', toSummaryEventData summary) with
        | true -> return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Ok (1, eventCount - 1)
        | false -> return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Skipped eventCount
    | _ -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.NotApplicable events.Length }
#endif

type Factory private () =
    
    static member StartSink(log, stats, maxConcurrentStreams, handle, maxReadAhead,
                            ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats,
                                                 ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
    
    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Store.Metrics.log) sink reactionCategories sourceConfig
