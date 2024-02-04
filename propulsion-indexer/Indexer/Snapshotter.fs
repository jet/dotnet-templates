module IndexerTemplate.Indexer.Snapshotter

open Visitor
open Propulsion.Internal

type Outcome = (struct (string * System.TimeSpan * Store.Snapshotter.Result))
module Outcome = let create sn ts res: Outcome = struct (FsCodec.StreamName.Category.ofStreamName sn, ts, res)

/// Gathers counts of snapshots updated vs skipped
type Stats(log, statsInterval, stateInterval, verboseStore, abendThreshold) =
    inherit StatsBase<Outcome>(log, statsInterval, stateInterval, verboseStore, abendThreshold = abendThreshold)
    let lats, accLats = Stats.LatencyStatsSet(), Stats.LatencyStatsSet()
    let counts, accCounts = CategoryCounters(), CategoryCounters()
    override _.HandleOk((cat, ts, res)) =
        lats.Record(cat, ts)
        accLats.Record(cat, ts)
        let count = [ FsCodec.Union.caseName res, 1 ]
        counts.Ingest(cat, count)
        accCounts.Ingest(cat, count)

    override _.DumpStats() =
        counts.DumpGrouped(log, "OUTCOMES")
        counts.Clear()
        lats.DumpGrouped(id, log, totalLabel = "CATEGORIES")
        lats.Clear()
        base.DumpStats()
    override _.DumpState(prune) =
        accCounts.DumpGrouped(log, "ΣOUTCOMES")
        accLats.DumpGrouped(id, log, totalLabel = "ΣCATEGORIES")
        if prune then accLats.Clear()
        base.DumpState(prune)

open IndexerTemplate.Domain

let handle todo
        stream _events: Async<_ * Outcome> = async {
    let ts = Stopwatch.timestamp ()
    let! res, pos' =
        match stream with
        | Todo.Reactions.For id -> todo id
        | sn -> failwith $"Unexpected category %A{sn}"
    // a) if the tryUpdate saw a version beyond what (Propulsion.Sinks.Events.nextIndex events) would suggest, then we pass that information out
    //    in order to have the scheduler drop all events until we meet an event that signifies we may need to re-update
    // b) the fact that we use the same Microsoft.Azure.Cosmos.CosmosClient for the Change Feed and the Equinox-based Services means we are guaranteed
    //    to always see all the _events we've been supplied. (Even if this were not the case, the scheduler would retain the excess events, and that
    //    would result in an immediate re-triggering of the handler with those events)
    let elapsed = Stopwatch.elapsed ts
    return Propulsion.Sinks.StreamResult.OverrideNextIndex pos', Outcome.create stream elapsed res }

module Factory =

    let createHandler dryRun store =

        let todo = Todo.Factory.createSnapshotter store

        let h svc = Store.Snapshotter.Service.tryUpdate dryRun svc
        handle
            (   h todo)
