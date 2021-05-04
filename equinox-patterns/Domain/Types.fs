namespace Patterns.Domain

open FSharp.UMX
type EpochId = int<epochId>
and [<Measure>] epochId

module EpochId =

    let parse (value : int) : EpochId = %value
    let tryPrev (value : EpochId) : EpochId option = match %value with 0 -> None | x -> Some %(x - 1)
    let next (value : EpochId) : EpochId = %(%value + 1)
    let toString (value : EpochId) : string = string %value
