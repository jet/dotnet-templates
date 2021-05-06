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

/// TODO replace the terms `tranche`, `trancheId` and type `TrancheId` with domain term that represents the nature of the separation
///      i.e. it might be a TenantId or a FulfilmentCenterId
type ItemTrancheId = string<itemTrancheId>
and [<Measure>] itemTrancheId
module ItemTrancheId =

    let parse (value : string) : ItemTrancheId = %value
    let toString (value : ItemTrancheId) : string = %value

/// Identifies an Epoch that holds a set of Items (within a Tranche)
/// TODO replace `Item` with a Domain term referencing the specific element being managed
type ItemEpochId = int<itemEpochId>
and [<Measure>] itemEpochId
module ItemEpochId =

    let unknown = -1<itemEpochId>
    let initial = 0<itemEpochId>
    let next (value : ItemEpochId) : ItemEpochId = % (%value + 1)
    let value (value : ItemEpochId) : int = %value
    let toString (value : ItemEpochId) : string = string %value

/// Identifies an Item stored within an Epoch
/// TODO replace `Item` with a Domain term referencing the specific element being managed

type ItemId = string<itemId>
and [<Measure>] itemId
module ItemId =

    let parse (value : string) : ItemId = %value
    let toString (value : ItemId) : string = %value

/// Identifies a group of chained Epochs (pe Tranche within the series)
/// TODO replace `Item` with a Domain term referencing the specific element being managed
type [<Measure>] itemSeriesId
type ItemSeriesId = string<itemSeriesId>
module ItemSeriesId =

    let wellKnownId : ItemSeriesId = % "0"
    let toString (value : ItemSeriesId) : string = %value
