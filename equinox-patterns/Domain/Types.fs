namespace Patterns.Domain

open FSharp.UMX

/// Identifies a single period within a temporally linked chain of periods
/// Each Period commences with a Balance `BroughtForward` based on what the predecessor Period
/// has decided should be `CarriedForward`
/// TODO prefix the name with a relevant Domain term
type PeriodId = int<periodId>
and [<Measure>] periodId
module PeriodId =

    let parse: int -> PeriodId = UMX.tag
    let tryPrev (value: PeriodId): PeriodId option = match UMX.untag value with 0 -> None | x -> Some (UMX.tag(x - 1))
    let next (value: PeriodId): PeriodId = UMX.tag (UMX.untag value + 1)
    let toString: PeriodId -> string = UMX.untag >> string

/// Identifies an Epoch that holds a list of Items
/// TODO replace `List` with a Domain term referencing the specific element being managed
type ListEpochId = int<listEpochId>
and [<Measure>] listEpochId
module ListEpochId =

    let initial: ListEpochId = UMX.tag 0
//    let value: ListEpochId -> int = UMX.untag
    let toString: ListEpochId -> string = UMX.untag >> string

/// Identifies an Item stored within an Epoch
/// TODO replace `Item` with a Domain term referencing the specific element being managed
type ItemId = string<itemId>
and [<Measure>] itemId
module ItemId =

    let parse: string -> ItemId = UMX.tag
    let toString: ItemId -> string = UMX.untag

/// Identifies a group of chained Epochs
/// TODO replace `List` with a Domain term referencing the specific element being managed
type [<Measure>] listSeriesId
type ListSeriesId = string<listSeriesId>
module ListSeriesId =

    let wellKnownId: ListSeriesId = UMX.tag "0"
    let toString: ListSeriesId -> string = UMX.untag

namespace global

module Guid =

    let toStringN (g: System.Guid) = g.ToString "N"
