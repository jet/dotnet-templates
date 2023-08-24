module ReactorTemplate.Handler

//#if kafka
[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used: int * unused: int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count: int
    /// Handler determined the events were not relevant to its duties and performed no decoding or processing
    | NotApplicable of count: int

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval, verboseStore, ?logExternalStats) =
#if (blank || sourceKafka)
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)
#else
    inherit Propulsion.Sync.Stats<Outcome>(log, statsInterval, stateInterval)
#endif

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
        | Equinox.CosmosStore.Exceptions.ServiceUnavailable when not verboseStore -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) = log.Information(exn, "Unhandled")

let generate stream version summary =
    let event = Contract.encode summary
    Propulsion.Codec.NewtonsoftJson.RenderedSummary.ofStreamEvent stream version event

#if blank
let categories = [| Contract.Input.Category |]
    
let handle
        (produceSummary: Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<unit>)
        stream events = async {
    match struct (stream, events) with
    | Contract.Input.Parse (_clientId, events) ->
        for version, event in events do
            let summary =
                match event with
                | Contract.Input.EventA { field = x } -> Contract.EventA { value = x }
                | Contract.Input.EventB { field = x } -> Contract.EventB { value = x }
            let wrapped = generate stream version summary
            let! _ = produceSummary wrapped in ()
        return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.Ok (events.Length, 0)
    | _ -> return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.NotApplicable events.Length }
#else
let categories = Todo.Reactions.categories
    
let handle
        (service: Todo.Service)
        (produceSummary: Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<unit>)
        stream events = async {
    match struct (stream, events) with
    | Todo.Reactions.ImpliesStateChange (clientId, eventCount) ->
        let! version', summary = service.QueryWithVersion(clientId, Contract.ofState)
        let wrapped = generate stream version' (Contract.Summary summary)
        let! _ = produceSummary wrapped
        return Propulsion.Sinks.StreamResult.OverrideNextIndex version', Outcome.Ok (1, eventCount - 1)
    | Todo.Reactions.NoStateChange eventCount ->
        return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.Skipped eventCount
    | Todo.Reactions.NotApplicable eventCount ->
        return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.NotApplicable eventCount }
#endif

type Factory private () =
    
    static member StartSink(log, stats, maxConcurrentStreams, handle, maxReadAhead,
                            ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats,
                                                 ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
    
    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Store.Metrics.log) sink categories sourceConfig
//#endif
