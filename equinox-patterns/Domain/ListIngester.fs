module Patterns.Domain.ListIngester

type Service internal (tip : TipIngester.Service<_, _, _>) =

    member _.IngestItems(originEpochId, items : ItemId[]) =
        tip.IngestMany(originEpochId, items)

module Config =

    let create_ linger maxItemsPerEpoch store =
        let series = ListSeries.Config.create store
        let epochs = ListEpoch.Config.create maxItemsPerEpoch store
        let tip = TipIngester.create linger (series.ReadIngestionEpochId, series.MarkIngestionEpochId) (epochs.Ingest, Array.toSeq)
        Service(tip)
    let create store =
        let defaultLinger = System.TimeSpan.FromMilliseconds 200.
        let maxItemsPerEpoch = 10_000
        create_ defaultLinger maxItemsPerEpoch store
