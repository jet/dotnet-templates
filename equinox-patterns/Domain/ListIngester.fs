module Patterns.Domain.ListIngester

type Service internal (ingester : ExactlyOnceIngester.Service<_, _, _, _>) =

    /// Slot the item into the series of epochs.
    /// Returns items that actually got added (i.e. may be empty if it was an idempotent retry).
    member _.IngestItems(originEpochId, items : ItemId[]) : Async<seq<ItemId>>=
        ingester.IngestMany(originEpochId, items)

    /// Efficiently determine a valid ingestion origin epoch
    member _.ActiveIngestionEpochId() =
        ingester.ActiveIngestionEpochId()

module Config =

    let create_ linger maxItemsPerEpoch store =
        let log = Serilog.Log.ForContext<Service>()
        let series = ListSeries.Config.create store
        let epochs = ListEpoch.Config.create maxItemsPerEpoch store
        let ingester = ExactlyOnceIngester.create log linger (series.ReadIngestionEpochId, series.MarkIngestionEpochId) (epochs.Ingest, Array.toSeq)
        Service(ingester)
    let create store =
        let defaultLinger = System.TimeSpan.FromMilliseconds 200.
        let maxItemsPerEpoch = 10_000
        create_ defaultLinger maxItemsPerEpoch store
