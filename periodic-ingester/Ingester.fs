module PeriodicIngesterTemplate.Ingester

open PeriodicIngesterTemplate.Domain
open System

[<RequireQualifiedAccess>]
type Outcome = Stale | Unchanged | Ok

/// Gathers stats based on the outcome of each Span processed for periodic emission
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable stale, unchanged, ok = 0, 0, 0

    override _.HandleOk outcome = outcome |> function
        | Outcome.Stale ->      stale <- stale + 1
        | Outcome.Unchanged ->  unchanged <- unchanged + 1
        | Outcome.Ok ->         ok <- ok + 1
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

    override _.DumpStats () =
        if stale <> 0 || unchanged <> 0 || ok <> 0 then
            log.Information(" Ingested {ok} Unchanged {unchanged} Stale {stale}", ok, unchanged, stale)
            stale <- 0; unchanged <- 0; ok <- 0

type TicketData = { lastUpdated : DateTimeOffset; body : string }

module PipelineEvent =

    let [<Literal>] Category = "Ticket"
    let streamName = TicketId.toString >> FsCodec.StreamName.create Category
    let (|StreamName|_|) = function
        | FsCodec.StreamName.CategoryAndId (Category, TicketId.Parse id) -> Some id
        | _ -> None
    let sourceItemOfTicketIdAndData (id : TicketId) (data : TicketData) : Propulsion.Feed.SourceItem =
        {   streamName = streamName id
            eventData = FsCodec.Core.EventData.Create("eventType", (* no body*) null) 
            context = box data }
    let (|TicketData|_|) = function
        | StreamName ticketId, (s : Propulsion.Streams.StreamSpan<_>) ->
            Some (ticketId, s.events |> Seq.map (fun e -> Unchecked.unbox<TicketData> e.Context))
        | _ -> None

let handle (stream, span) = async {
    match stream, span with
    | PipelineEvent.TicketData (ticketId, data) ->
        // TODO : Ingest the data
        return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Ok
    | x -> return failwithf "Unexpected stream %O" x
}
