/// Handles the ingestion of events supplied by the projector into a store
module PeriodicIngesterTemplate.Ingester.Ingester

open PeriodicIngesterTemplate.Domain
open System

/// Gathers stats based on the outcome of each Span processed for periodic emission
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<IngestionOutcome>(log, statsInterval, stateInterval)

    let mutable stale, unchanged, changed = 0, 0, 0
    override _.HandleOk outcome =
        Prometheus.Stats.observeIngestionOutcome outcome
        match outcome with
        | IngestionOutcome.Stale ->      stale <- stale + 1
        | IngestionOutcome.Unchanged ->  unchanged <- unchanged + 1
        | IngestionOutcome.Changed ->    changed <- changed + 1
    override _.DumpStats() =
        base.DumpStats()
        if stale <> 0 || unchanged <> 0 || changed <> 0 then
            log.Information(" Changed {changed} Unchanged {skipped} Stale {stale}", changed, unchanged, stale)
            stale <- 0; unchanged <- 0; changed <- 0

    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

type TicketData = { lastUpdated: DateTimeOffset; body: string }

module PipelineEvent =

    (* Each item fed into the Sink has a StreamName associated with it, just as with a regular source based on a change feed *)

    let [<Literal>] CategoryName = "Ticket"
    let private streamId = FsCodec.StreamId.gen TicketId.toString
    let private catId = CategoryId(CategoryName, streamId, FsCodec.StreamId.dec TicketId.parse)
    let [<return: Struct>] (|For|_|) = catId.TryDecode

    (* Each item per stream is represented as an event; if multiple events have been found for a given stream, they are delivered together *)

    let private dummyEventData = let dummyEventType, noBody = "eventType", Unchecked.defaultof<_> in FsCodec.Core.EventData.Create(dummyEventType, noBody)
    let sourceItemOfTicketIdAndData struct (id: TicketId, data: TicketData): Propulsion.Feed.SourceItem<Propulsion.Sinks.EventBody> =
        { streamName = catId.StreamName id; eventData = dummyEventData; context = box data }
    let [<return: Struct>] (|TicketEvents|_|) = function
        | For ticketId, (s: Propulsion.Sinks.Event[]) ->
            ValueSome (ticketId, s |> Seq.map (fun e -> Unchecked.unbox<TicketData> e.Context))
        | _ -> ValueNone

let handle stream events = async {
    match stream, events with
    | PipelineEvent.TicketEvents (ticketId, items) ->
        // TODO : Ingest the data
        return Propulsion.Sinks.StreamResult.AllProcessed, IngestionOutcome.Unchanged
    | x -> return failwithf "Unexpected stream %O" x
}

type Factory private () =
    
    static member StartSink(log: Serilog.ILogger, stats, maxConcurrentStreams, handle, maxReadAhead) =
        Propulsion.Sinks.Factory.StartConcurrent(log, maxReadAhead, maxConcurrentStreams, handle, stats)
