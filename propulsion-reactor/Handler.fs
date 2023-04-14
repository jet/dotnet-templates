module ReactorTemplate.Handler

//#if kafka
[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used : int * unused : int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count : int
    /// Handler determined the events were not relevant to its duties and performed no decoding or processing
    | NotApplicable of count : int

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval, verboseStore, ?logExternalStats) =
#if (blank || sourceKafka)
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)
#else
    inherit Propulsion.Streams.Sync.Stats<Outcome>(log, statsInterval, stateInterval)
#endif

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
    override _.HandleExn(log, exn) = log.Information(exn, "Unhandled")

    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" used {ok} skipped {skipped} n/a {na}", ok, skipped, na)
            ok <- 0; skipped <- 0; na <- 0
        logExternalStats |> Option.iter (fun dumpTo -> dumpTo log)

let generate stream version summary =
    let event = Contract.encode summary
    Propulsion.Codec.NewtonsoftJson.RenderedSummary.ofStreamEvent stream version event

#if blank
let reactionCategories = [| Contract.Input.Category |]
    
let handle
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<unit>)
        stream span = async {
    match stream, span with
    | Contract.Input.Parse (_clientId, events) ->
        for version, event in events do
            let summary =
                match event with
                | Contract.Input.EventA { field = x } -> Contract.EventA { value = x }
                | Contract.Input.EventB { field = x } -> Contract.EventB { value = x }
            let wrapped = generate stream version summary
            let! _ = produceSummary wrapped in ()
        return struct (Propulsion.Streams.SpanResult.AllProcessed, Outcome.Ok (events.Length, 0))
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.Length }
#else
let categories = [| Todo.Reactions.Category |]
    
let handle
        (service : Todo.Service)
        (produceSummary : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<unit>)
        stream span = async {
    match stream, span with
    | Todo.Reactions.Parse (clientId, events) ->
        if events |> Seq.exists Todo.Reactions.impliesStateChange then
            let! version', summary = service.QueryWithVersion(clientId, Contract.ofState)
            let wrapped = generate stream version' (Contract.Summary summary)
            let! _ = produceSummary wrapped
            return struct (Propulsion.Streams.SpanResult.OverrideWritePosition version', Outcome.Ok (1, events.Length - 1))
        else
            return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Skipped events.Length
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.Length }
#endif

type Config private () =
    
    static member StartSink(log : Serilog.ILogger, stats : Propulsion.Streams.Scheduling.Stats<_, _>, maxConcurrentStreams : int,
                            handle : _ -> _ -> Async<_>,
                            maxReadAhead : int, ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Streams.Default.Config.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval.Period,
                                                ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)
    
    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Config.log) sink categories sourceConfig
//#endif
