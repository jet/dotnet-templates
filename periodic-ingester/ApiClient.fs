/// Provides logic for crawling of the source dataset
/// (Wrapped in a PeriodicSource to manage continual refreshing and checkpointing when a traverse has completed)
module PeriodicIngesterTemplate.Ingester.ApiClient

open FSharp.Control
open System
open System.Net.Http

open PeriodicIngesterTemplate.Domain

[<NoComparison; NoEquality>]
type TicketsDto = { tickets : TicketDto[] }
 and TicketDto = { id : TicketId; lastUpdated : DateTimeOffset; body : string; }

type TicketsClient(client : HttpClient) =

    let basePath = "api/tickets"

    member _.Crawl() : AsyncSeq<struct (TimeSpan * Propulsion.Feed.SourceItem<Propulsion.Streams.Default.EventBody> array)> = asyncSeq {
        let request = HttpReq.get () |> HttpReq.withPath basePath
        let ts = System.Diagnostics.Stopwatch.StartNew()
        let! response = client.Send2(request)
        let! basePage = response |> HttpRes.deserializeOkStj<TicketsDto>
        yield ts.Elapsed,
            [| for t in basePage.tickets ->
                let data : Ingester.TicketData = { lastUpdated = t.lastUpdated; body = t.body }
                Ingester.PipelineEvent.sourceItemOfTicketIdAndData (t.id, data) |]
    }

type TicketsFeed(baseUri) =

    let client = new HttpClient(BaseAddress = baseUri)
    let tickets = TicketsClient(client)

    // TODO add retries - consumer loop will abort if this throws
    member _.Crawl(_trancheId): AsyncSeq<struct (TimeSpan * Propulsion.Feed.SourceItem<_> array)> =
        tickets.Crawl()
