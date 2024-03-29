module ProjectorTemplate.Handler

open Propulsion.Internal
//#if cosmos
#if     parallelOnly
// Here we pass the items directly through to the handler without parsing them
let categories = [||] // TODO add category names
#endif // !parallelOnly
//#endif // cosmos

#if kafka
#if     (cosmos && parallelOnly) // kafka && cosmos && parallelOnly
type ExampleOutput = { id: string }

let serdes = FsCodec.SystemTextJson.Options.Default |> FsCodec.SystemTextJson.Serdes
let render ((_s,e): Propulsion.Sinks.StreamEvent) =
    let doc = unbox<System.Text.Json.JsonDocument> e.Context
    let r = doc.RootElement
    let gs (name: string) = let x = r.GetProperty name in x.GetString()
    let equinoxPartition, itemId = gs "p", gs "id"
    struct (equinoxPartition, serdes.Serialize { id = itemId })
#else // kafka && !(cosmos && parallelOnly)
// Each outcome from `handle` is passed to `HandleOk` or `HandleExn` by the scheduler, DumpStats is called at `statsInterval`
// The incoming calls are all sequential - the logic does not need to consider concurrent incoming calls
type ProductionStats(log, statsInterval, stateInterval) =
    inherit Propulsion.Sync.Stats<unit>(log, statsInterval, stateInterval)

    // TODO consider whether it's warranted to log every time a message is produced given the stats will periodically emit counts
    override _.HandleOk(()) =
        log.Warning("Produced")
    // TODO consider whether to log cause of every individual produce failure in full (Failure counts are emitted periodically)
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

/// Responsible for wrapping a span of events for a specific stream into an envelope
/// The well-defined Propulsion.Codec `RenderedSpan` represents the accumulated span of events for a given stream as an
///   array within each message in order to maximize throughput within constraints Kafka's model implies (we are aiming
///   to preserve ordering at stream (key) level for messages produced to the topic)
// TODO NOTE: The bulk of any manipulation should take place before events enter the scheduler, i.e. in program.fs
// TODO NOTE: While filtering out entire categories is appropriate, you should not filter within a given stream (i.e., by event type)
let render (stream: FsCodec.StreamName) (events: Propulsion.Sinks.Event[]) = async {
    let value =
        events
        |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.ofStreamSpan stream
        |> Propulsion.Codec.NewtonsoftJson.Serdes.Serialize
    return FsCodec.StreamName.toString stream, value }

let categories = [||] // TODO add category names to render

#endif // kafka && !(cosmos && parallelOnly)
#else // !kafka
// Each outcome from `handle` is passed to `HandleOk` or `HandleExn` by the scheduler, DumpStats is called at `statsInterval`
// The incoming calls are all sequential - the logic does not need to consider concurrent incoming calls
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<int>(log, statsInterval, stateInterval)

    let mutable totalCount = 0
    // TODO consider best balance between logging or gathering summary information per handler invocation
    // here we don't log per invocation (such high level stats are already gathered and emitted) but accumulate for periodic emission
    override _.HandleOk count =
        totalCount <- totalCount + count
    override _.DumpStats() =
        base.DumpStats()
        log.Information(" Total events processed {total}", totalCount)
        totalCount <- 0

    // TODO consider whether to log cause of every individual failure in full (Failure counts are emitted periodically)
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

let categories = [| "categoryA" |]

let handle _stream (events: Propulsion.Sinks.Event[]) = async {
    let r = System.Random()
    let ms = r.Next(1, events.Length)
    do! Async.Sleep ms
    return Propulsion.Sinks.StreamResult.AllProcessed, events.Length }
#endif // !kafka

type Factory private () =
    
    static member StartSink(log, stats, maxConcurrentStreams, handle, maxReadAhead,
                            ?wakeForResults, ?idleDelay, ?purgeInterval) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats,
                                                 ?wakeForResults = wakeForResults, ?idleDelay = idleDelay, ?purgeInterval = purgeInterval)

    static member StartSource(log, sink, sourceConfig) =
        SourceConfig.start (log, Store.Metrics.log) sink categories sourceConfig
