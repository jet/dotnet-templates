namespace FeedSourceTemplate.Domain

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guid+strings

type [<Measure>] fcId
type FcId = string<fcId>
module FcId =
    let toString (value : FcId) : string = %value
    let parse (value : string) : FcId = let raw = value in % raw
    let (|Parse|) = parse

type [<Measure>] ticketId
type TicketId = string<ticketId>
module TicketId =
    let toString (value : TicketId) : string = %value
    let parse (value : string) : TicketId = let raw = value in % raw
    let (|Parse|) = parse

(* Identifies an epoch of the series of streams bearing ticket references within our dataset *)

type [<Measure>] ticketsEpochId
type TicketsEpochId = int<ticketsEpochId>
module TicketsEpochId =
    let unknown = -1<ticketsEpochId>
    let initial = 0<ticketsEpochId>
    let value (value : TicketsEpochId) : int = %value
    let next (value : TicketsEpochId) : TicketsEpochId = % (%value + 1)
    let toString (value : TicketsEpochId) : string = string %value

(* Identifies the series pointing to the active TicketsEpochId per FC *)

type [<Measure>] ticketsSeriesId
type TicketsSeriesId = string<ticketsSeriesId>
module TicketsSeriesId =
    let wellKnownId : TicketsSeriesId = % "0"
    let toString (value : TicketsSeriesId) : string = %value

type [<Measure>] ticketsCheckpoint
type TicketsCheckpoint = int64<ticketsCheckpoint>
module TicketsCheckpoint =

    let initial : TicketsCheckpoint = %0L

    let ofEpochAndOffset (epoch : TicketsEpochId) offset : TicketsCheckpoint =
        int64 (TicketsEpochId.value epoch) * 1_000_000L + int64 offset |> UMX.tag

    let ofEpochContent (epoch : TicketsEpochId) isClosed count : TicketsCheckpoint =
        let epoch, offset =
            if isClosed then TicketsEpochId.next epoch, 0
            else epoch, count
        ofEpochAndOffset epoch offset

    let toEpochAndOffset (value : TicketsCheckpoint) : TicketsEpochId * int =
        let d, r = System.Math.DivRem(%value, 1_000_000L)
        (%int %d : TicketsEpochId), int r
