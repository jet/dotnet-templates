namespace FeedApiTemplate.Controllers

open Microsoft.AspNetCore.Mvc

open FeedApiTemplate.Domain

type TicketsTranchesDto = { activeEpochs : TrancheReferenceDto[] }
and TrancheReferenceDto = { fc : FcId; epochId : TicketsEpochId }

type SliceDto = { closed : bool; tickets : TicketId[]; position : TicketsCheckpoint; checkpoint : TicketsCheckpoint }

module Checkpoint =

    let ofEpochAndOffset (epoch : TicketsEpochId) (offset : int) =
        TicketsCheckpoint.ofEpochAndOffset epoch offset

    let ofState (epochId : TicketsEpochId) (s : TicketsEpoch.StateDto) =
        TicketsCheckpoint.ofEpochContent epochId s.closed s.tickets.Length

[<ApiController>]
[<Route("api/[controller]")>]
type TicketsController(tickets : Tickets.Service, series : TicketsSeries.Service, epochs : TicketsEpoch.ReadService) =
    inherit ControllerBase()

    [<HttpPost; Route "{fc}/{ticket}">]
    member _.Post(fc : FcId, ticket : TicketId) = async {
        let! _added = tickets.ForFc(fc).TryIngest(ticket)
        ()
    }

    [<HttpGet>]
    member _.ListTranches() : Async<TicketsTranchesDto> = async {
        let! active = series.ReadIngestionEpochs()
        return { activeEpochs = [| for x in active -> { fc = x.fc; epochId = x.ingestionEpochId } |]}
    }

    [<HttpGet; Route("{fcId}/{epoch}")>]
    member _.ReadTranche(fcId : FcId, epoch : TicketsEpochId) : Async<SliceDto> = async {
        let! state = epochs.Read(fcId, epoch)
        // TOCONSIDER closed should control cache header
        let pos, checkpoint = Checkpoint.ofEpochAndOffset epoch 0, Checkpoint.ofState epoch state
        return { closed = state.closed; tickets = state.tickets; position = pos; checkpoint = checkpoint }
    }

    [<HttpGet; Route("{fcId}/slice/{token?}")>]
    member _.Poll(fcId : FcId, token : System.Nullable<TicketsCheckpoint>) : Async<SliceDto> = async {
        let pos = if token.HasValue then token.Value else TicketsCheckpoint.initial
        let epochId, offset = TicketsCheckpoint.toEpochAndOffset pos
        let! state = epochs.Read(fcId, epochId)
        // TOCONSIDER closed should control cache header
        let pos, checkpoint = Checkpoint.ofEpochAndOffset epochId offset, Checkpoint.ofState epochId state
        return { closed = state.closed; tickets = Array.skip offset state.tickets; position = pos; checkpoint = checkpoint }
    }
