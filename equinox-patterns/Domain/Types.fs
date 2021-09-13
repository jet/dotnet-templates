namespace Patterns.Domain

open FSharp.UMX

/// Identifies a single period within a temporally linked chain of periods
/// Each Period commences with a Balance `BroughtForward` based on what the predecessor Period
/// has decided should be `CarriedForward`
/// TODO prefix the name with a relevant Domain term
type PeriodId = int<periodId>
and [<Measure>] periodId
module PeriodId =

    let parse (value : int) : PeriodId = %value
    let tryPrev (value : PeriodId) : PeriodId option = match %value with 0 -> None | x -> Some %(x - 1)
    let next (value : PeriodId) : PeriodId = %(%value + 1)
    let toString (value : PeriodId) : string = string %value

/// Identifies an Epoch that holds a list of Items
/// TODO replace `List` with a Domain term referencing the specific element being managed
type ListEpochId = int<listEpochId>
and [<Measure>] listEpochId
module ListEpochId =

    let unknown = -1<listEpochId>
    let initial = 0<listEpochId>
    let next (value : ListEpochId) : ListEpochId = % (%value + 1)
    let value (value : ListEpochId) : int = %value
    let toString (value : ListEpochId) : string = string %value

/// Identifies an Item stored within an Epoch
/// TODO replace `Item` with a Domain term referencing the specific element being managed

type ItemId = string<itemId>
and [<Measure>] itemId
module ItemId =

    let parse (value : string) : ItemId = %value
    let toString (value : ItemId) : string = %value

/// Identifies a group of chained Epochs
/// TODO replace `List` with a Domain term referencing the specific element being managed
type [<Measure>] listSeriesId
type ListSeriesId = string<listSeriesId>
module ListSeriesId =

    let wellKnownId : ListSeriesId = % "0"
    let toString (value : ListSeriesId) : string = %value
