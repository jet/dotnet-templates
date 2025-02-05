module IndexerTemplate.Indexer.Ingester

open IndexerTemplate.Domain
open Visitor

type Stats(log, statsInterval, stateInterval, verboseStore, abendThreshold) =
    inherit StatsBase<unit>(log, statsInterval, stateInterval, verboseStore, abendThreshold = abendThreshold)
    override _.HandleOk(()) = ()

let handle todo
        stream (events: Propulsion.Sinks.Event[]): Async<unit * int64> = async {
    let handle =
        match stream with
        | Todo.Reactions.For id -> todo id
        | sn -> failwith $"Unexpected category %A{sn}"
    let! pos' = handle events
    return (), pos' }

module Factory =

    let createHandler store =

        let todo = Todo.Factory.createIngester store

        let h svc = Store.Ingester.Service.ingest svc
        handle
            (h todo)
