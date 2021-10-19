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
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

    override _.DumpStats () =
        if stale <> 0 || unchanged <> 0 || changed <> 0 then
            log.Information(" Changed {changed} Unchanged {skipped} Stale {stale}", changed, unchanged, stale)
            stale <- 0; unchanged <- 0; changed <- 0

type TicketData = { lastUpdated : DateTimeOffset; body : string }

module PipelineEvent =

    (* Each item fed into the Sink has a StreamName associated with it, just as with a regular source based on a change feed *)

    let [<Literal>] Category = "Ticket"
    let streamName = TicketId.toString >> FsCodec.StreamName.create Category
    let (|StreamName|_|) = function
        | FsCodec.StreamName.CategoryAndId (Category, TicketId.Parse id) -> Some id
        | _ -> None

    (* Each item per stream is represented as an event; if multiple events have been found for a given stream, they are delivered together *)

    let private dummyEventData = let dummyEventType, noBody = "eventType", null in FsCodec.Core.EventData.Create(dummyEventType, noBody)
    let sourceItemOfTicketIdAndData (id : TicketId, data : TicketData) : Propulsion.Feed.SourceItem =
        { streamName = streamName id; eventData = dummyEventData; context = box data }
    let (|TicketEvents|_|) = function
        | StreamName ticketId, (s : Propulsion.Streams.StreamSpan<_>) ->
            Some (ticketId, s.events |> Seq.map (fun e -> Unchecked.unbox<TicketData> e.Context))
        | _ -> None

let handle (stream, span) = async {
    match stream, span with
    | PipelineEvent.TicketEvents (ticketId, items) ->
        // TODO : Ingest the data
        return Propulsion.Streams.SpanResult.AllProcessed, IngestionOutcome.Unchanged
    | x -> return failwithf "Unexpected stream %O" x
}
