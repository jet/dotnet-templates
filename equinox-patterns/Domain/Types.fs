namespace Patterns.Domain

open FSharp.UMX

type PeriodId = int<periodId>
and [<Measure>] periodId

module PeriodId =

    let parse (value : int) : PeriodId = %value
    let tryPrev (value : PeriodId) : PeriodId option = match %value with 0 -> None | x -> Some %(x - 1)
    let next (value : PeriodId) : PeriodId = %(%value + 1)
    let toString (value : PeriodId) : string = string %value
