module IndexerTemplate.Indexer.Snapshotter

type Outcome = Store.Snapshotter.Result
module Outcome = let create res: Outcome = res

type Stats(log, statsInterval, stateInterval, verboseStore) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable handled, skipped = 0, 0
    override _.HandleOk(res) = if res = Store.Snapshotter.Updated then handled <- handled + 1 else skipped <- skipped + 1
    override _.DumpStats() =
        base.DumpStats()
        log.Information(" Snapshotted {handled}, skipped {skipped}", handled, skipped)
        handled <- 0; skipped <- 0
        Equinox.CosmosStore.Core.Log.InternalMetrics.dump Serilog.Log.Logger

    override _.Classify(e) =
        match e with
        | OutcomeKind.StoreExceptions kind -> kind
        | Equinox.CosmosStore.Exceptions.ServiceUnavailable when not verboseStore -> Propulsion.Streams.OutcomeKind.RateLimited
        | x -> base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

open IndexerTemplate.Domain

let handle
        todo
        stream _events = async {
    let! res, pos' =
        match stream with
        | Todo.Reactions.For id -> todo id
        | sn -> failwith $"Unexpected category %A{sn}"
    // a) if the tryUpdate saw a version beyond what (Propulsion.Sinks.Events.nextIndex events) would suggest, then we pass that information out
    //    in order to have the scheduler drop all events until we meet an event that signifies we may need to re-update
    // b) the fact that we use the same Microsoft.Azure.Cosmos.CosmosClient for the Change Feed and the Equinox-based Services means we are guaranteed
    //    to always see all the _events we've been supplied. (Even if this were not the case, the scheduler would retain the excess events, and that
    //    would result in an immediate re-triggering of the handler with those events)
    return res, pos' }

module Factory =

    let createHandler dryRun store =

        let forceLoadingAndFoldingAllEvents = dryRun
        let h createSnapshotterCategory = Store.Snapshotter.Service.tryUpdate dryRun (createSnapshotterCategory store forceLoadingAndFoldingAllEvents)
        handle
            (   h Todo.Factory.createSnapshotter)
