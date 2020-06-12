namespace Fc.Domain.Location

[<NoComparison; NoEquality>]
type Wip<'R> =
    | Pending of decide : (Epoch.Fold.State -> 'R * Epoch.Events.Event list)
    | Complete of 'R

/// Manages Reads and Writes for a Series of Epochs, with a running total being carried forward to the next Epoch when it's marked Closed
type Service internal (zeroBalance, toBalanceCarriedForward, shouldClose, series : Series.Service, epochs : Epoch.Service) =

    let execute locationId originEpochId =
        let rec aux epochId balanceToCarryForward wip = async {
            let decide state = match wip with Complete r -> r, [] | Pending decide -> decide state
            match! epochs.Sync(locationId, epochId, balanceToCarryForward, decide, shouldClose) with
            | { result = Some res; isOpen = true } ->
                if originEpochId <> epochId then
                    do! series.AdvanceIngestionEpoch(locationId, epochId)
                return res
            | { history = history; result = Some res } ->
                let successorEpochId = LocationEpochId.next epochId
                let cf = toBalanceCarriedForward history
                return! aux successorEpochId (Some cf) (Complete res)
            | { history = history } ->
                let successorEpochId = LocationEpochId.next epochId
                let cf = toBalanceCarriedForward history
                return! aux successorEpochId (Some cf) wip }
        aux

    member __.Execute(locationId, decide) = async {
        let! activeEpoch = series.TryReadIngestionEpoch locationId
        let originEpochId, epochId, balanceCarriedForward =
            match activeEpoch with
            | None -> LocationEpochId.parse -1, LocationEpochId.parse 0, Some zeroBalance
            | Some activeEpochId -> activeEpochId, activeEpochId, None
        return! execute locationId originEpochId epochId balanceCarriedForward (Pending decide)}

[<AutoOpen>]
module Helpers =

    let create (zeroBalance, toBalanceCarriedForward, shouldClose) (series, epochs) =
        Service(zeroBalance, toBalanceCarriedForward, shouldClose, series, epochs)

module Cosmos =

    let create (zeroBalance, toBalanceCarriedForward, shouldClose) (context, cache, maxAttempts) =
        let series = Series.Cosmos.create (context, cache, maxAttempts)
        let epochs = Epoch.Cosmos.create (context, cache, maxAttempts)
        create (zeroBalance, toBalanceCarriedForward, shouldClose) (series, epochs)

module EventStore =

    let create (zeroBalance, toBalanceCarriedForward, shouldClose) (context, cache, maxAttempts) =
        let series = Series.EventStore.create (context, cache, maxAttempts)
        let epochs = Epoch.EventStore.create (context, cache, maxAttempts)
        create (zeroBalance, toBalanceCarriedForward, shouldClose) (series, epochs)
