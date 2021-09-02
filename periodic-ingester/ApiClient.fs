/// Provides logic for crawling of the source dataset
/// (Wrapped in a PeriodicSource to manage continual refreshing and checkpointing when a traverse has completed)
module PeriodicIngesterTemplate.ApiClient

open FSharp.Control
open System
open System.Net.Http

open PeriodicIngesterTemplate.Domain

[<NoComparison; NoEquality>]
type TicketsDto = { tickets : TicketDto[] }
 and TicketDto = { id : TicketId; lastUpdated : DateTimeOffset; body : string; }

type TicketsClient(client : HttpClient) =

    let basePath = "api/tickets"

    member _.Crawl() : AsyncSeq<Propulsion.Feed.SourceItem[]> = asyncSeq {
        let request = HttpReq.get () |> HttpReq.withPath basePath
        let! response = client.Send request
        let! basePage = response |> HttpRes.deserializeOkJsonNet<TicketsDto>
        yield
            [| for t in basePage.tickets ->
                let data : Ingester.TicketData = { lastUpdated = t.lastUpdated; body = t.body }
                Ingester.PipelineEvent.sourceItemOfTicketIdAndData (t.id, data) |]
    }
    
type TicketsFeed(baseUri) =

    let client = new HttpClient(BaseAddress = baseUri)
    let tickets = TicketsClient(client)

    // TODO add retries - consumer loop will abort if this throws
    member _.Crawl(): AsyncSeq<Propulsion.Feed.SourceItem[]> =
        tickets.Crawl()
