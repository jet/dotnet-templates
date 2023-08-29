module IndexerTemplate.Indexer.Snapshotter

type Outcome = bool

type Stats(log, statsInterval, stateInterval, verboseStore) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable handled, skipped = 0, 0
    override _.HandleOk(updated) = if updated then handled <- handled + 1 else skipped <- skipped + 1
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
        tryUpdateTodo
        stream _events: Async<_ * Outcome> = async {
    let! res, pos' =
        match stream with
        | Todo.Reactions.For id -> tryUpdateTodo id
        | sn -> failwith $"Unexpected category %A{sn}"
    // a) if the tryUpdate saw a version beyond what (Propulsion.Sinks.Events.nextIndex events) would suggest, then we pass that information out
    //    in order to have the scheduler drop all events until we meet an event that signifies we may need to re-update
    // b) the fact that we use the same Microsoft.Azure.Cosmos.CosmosClient for the Change Feed and the Equinox-based Services means we are guaranteed
    //    to always see all the _events we've been supplied. (Even if this were not the case, the scheduler would retain the excess events, and that
    //    would result in an immediate re-triggering of the handler with those events)
    return Propulsion.Sinks.StreamResult.OverrideNextIndex pos', res }

module Factory =

    let createHandler context =

        let todo = Todo.Factory.createSnapshotter context
        handle
            todo.TryUpdate
