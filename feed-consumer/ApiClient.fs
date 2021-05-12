module FeedConsumerTemplate.ApiClient

open FSharp.UMX
open System
open System.Net.Http

open FeedConsumerTemplate.Domain

(* The feed presents a Tranche (series of epochs) per FC *)

module TrancheId =

    let toFcId (x : Propulsion.Feed.TrancheId) : FcId = %x
    let ofFcId (x : FcId) : Propulsion.Feed.TrancheId = %x

type TicketsEpochId = int<ticketsEpochId>
and [<Measure>] ticketsEpochId

[<NoComparison; NoEquality>]
type TicketsTranchesDto = { activeEpochs : TrancheReferenceDto[] }
and TrancheReferenceDto = { fc : FcId; epochId : TicketsEpochId }

(* Each Tranche response includes a checkpoint, which can be presented to Poll in order to resume consumption *)

type TicketsCheckpoint = int64<ticketsCheckpoint>
and [<Measure>] ticketsCheckpoint
module TicketsCheckpoint =
    let ofPosition (x : Propulsion.Feed.Position) : TicketsCheckpoint = %x
    let toPosition (x : TicketsCheckpoint) : Propulsion.Feed.Position = %x
    let toStreamIndex (x : TicketsCheckpoint) : int64 = %x

type SliceDto = { closed : bool; tickets : TicketId[]; position : TicketsCheckpoint; checkpoint : TicketsCheckpoint }

type Session(client: HttpClient) =

    member _.Send(req : HttpRequestMessage) : Async<HttpResponseMessage> =
        client.Send(req)

type TicketsClient(session: Session) =

    let basePath = "api/tickets"

    member _.ActiveFcs() : Async<FcId[]> = async {
        let request = HttpReq.get () |> HttpReq.withPath basePath
        let! response = session.Send request
        let! body = response |> HttpRes.deserializeOkJsonNet<TicketsTranchesDto>
        return [| for f in body.activeEpochs -> f.fc |]
    }

    member _.ReadPage(fc : FcId, index : int) : Async<SliceDto> = async {
        let request = HttpReq.post () |> HttpReq.withPathf "%s/%O/%d" basePath fc index
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<SliceDto>
    }

    member _.Poll(fc : FcId, checkpoint: TicketsCheckpoint) : Async<SliceDto> = async {
        let request = HttpReq.create () |> HttpReq.withPathf "%s/%O/slice/%O" basePath fc checkpoint
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<SliceDto>
    }

type Session with

    member session.Tickets = TicketsClient session

type TicketsFeed(baseUri) =

    let client = new HttpClient(BaseAddress = baseUri)
    let tickets = Session(client).Tickets

    // TODO add retries - consumer loop will abort if this throws
    member _.Poll(trancheId, pos) : Async<Propulsion.Feed.Page<byte[]>> = async {
        let checkpoint = TicketsCheckpoint.ofPosition pos
        let! pg = tickets.Poll(TrancheId.toFcId trancheId, checkpoint)
        let baseIndex = TicketsCheckpoint.toStreamIndex pg.position
        let items = pg.tickets |> Array.mapi (fun i -> Ingester.PipelineEvent.ofIndexAndTicketId (baseIndex + int64 i))
        return { checkpoint = TicketsCheckpoint.toPosition pg.checkpoint; items = items; isTail = not pg.closed }
    }

    // TODO add retries - consumer loop will not commence if this emits an exception
    member _.ReadTranches() : Async<Propulsion.Feed.TrancheId[]> = async {
        let! activeFcs = tickets.ActiveFcs()
        return [| for f in activeFcs -> TrancheId.ofFcId f |]
    }
