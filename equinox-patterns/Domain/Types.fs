namespace Patterns.Domain

open FSharp.UMX

type PeriodId = int<periodId>
and [<Measure>] periodId
module PeriodId =

    let parse (value : int) : PeriodId = %value
    let tryPrev (value : PeriodId) : PeriodId option = match %value with 0 -> None | x -> Some %(x - 1)
    let next (value : PeriodId) : PeriodId = %(%value + 1)
    let toString (value : PeriodId) : string = string %value

type ItemId = string<itemId>
and [<Measure>] itemId
module ItemId =

    let parse (value : string) : ItemId = %value
    let toString (value : ItemId) : string = %value

type TrancheId = string<trancheId>
and [<Measure>] trancheId
module TrancheId =

    let parse (value : string) : TrancheId = %value
    let toString (value : TrancheId) : string = %value

type EpochId = int<epochId>
and [<Measure>] epochId
module EpochId =

//    let unknown = -1<epochId>
//    let initial = 0<epochId>
//    let next (value : EpochId) : EpochId = % (%value + 1)
//    let value (value : EpochId) : int = %value
    let toString (value : EpochId) : string = string %value

type [<Measure>] seriesId
type SeriesId = string<seriesId>
module SeriesId =

    let wellKnownId : SeriesId = % "0"
    let toString (value : SeriesId) : string = %value
