/// Provides logic for crawling of the source dataset
/// (Wrapped in a PeriodicSource to manage continual refreshing and checkpointing when a traverse has completed)
module PeriodicIngesterTemplate.Ingester.ApiClient

open FSharp.Control
open System
open System.Collections.Generic
open System.Net.Http
open System.Threading

open PeriodicIngesterTemplate.Domain

[<NoComparison; NoEquality>]
type TicketsDto = { tickets : TicketDto[] }
 and TicketDto = { id : TicketId; lastUpdated : DateTimeOffset; body : string; }

type TicketsClient(client : HttpClient) =

    let basePath = "api/tickets"

    member _.Crawl(ct : CancellationToken) : IAsyncEnumerable<struct (TimeSpan * Propulsion.Feed.SourceItem<Propulsion.Streams.Default.EventBody> array)> = taskSeq {
        let request = HttpReq.get () |> HttpReq.withPath basePath
        let ts = System.Diagnostics.Stopwatch.StartNew()
        let! response = client.Send2(request, ct)
        let! basePage = response |> HttpRes.deserializeOkStj<TicketsDto>
        yield struct (ts.Elapsed,
            [| for t in basePage.tickets ->
                let data : Ingester.TicketData = { lastUpdated = t.lastUpdated; body = t.body }
                Ingester.PipelineEvent.sourceItemOfTicketIdAndData (t.id, data) |])
    }

type TicketsFeed(baseUri) =

    let client = new HttpClient(BaseAddress = baseUri)
    let tickets = TicketsClient(client)

    // TODO add retries - consumer loop will abort if this throws
    member _.Crawl(_trancheId): IAsyncEnumerable<struct (TimeSpan * Propulsion.Feed.SourceItem<_> array)> =
        tickets.Crawl(CancellationToken.None)
