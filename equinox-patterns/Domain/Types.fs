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

/// TODO replace the terms `tranche`, `trancheId` and type `TrancheId`
///      with a domain specific term that represents the nature of the separation
///      i.e. it might be a TenantId or a FulfilmentCenterId
type TrancheId = string<trancheId>
and [<Measure>] trancheId
module TrancheId =

    let parse (value : string) : TrancheId = %value
    let toString (value : TrancheId) : string = %value

/// Identifies an Epoch that holds a set of Items (within a Tranche)
/// TODO prefix name with a Domain term referencing the Item being managed
type EpochId = int<epochId>
and [<Measure>] epochId
module EpochId =

    let unknown = -1<epochId>
    let initial = 0<epochId>
    let next (value : EpochId) : EpochId = % (%value + 1)
    let value (value : EpochId) : int = %value
    let toString (value : EpochId) : string = string %value

/// Identifies an Item stored within an Epoch
/// TODO rename or prefix name with a Domain term referencing the Item being managed

type ItemId = string<itemId>
and [<Measure>] itemId
module ItemId =

    let parse (value : string) : ItemId = %value
    let toString (value : ItemId) : string = %value

/// Identifies a group of chained Epochs (pe Tranche within the series)
/// TODO prefix name with a Domain term referencing the Item being managed
type [<Measure>] seriesId
type SeriesId = string<seriesId>
module SeriesId =

    let wellKnownId : SeriesId = % "0"
    let toString (value : SeriesId) : string = %value
