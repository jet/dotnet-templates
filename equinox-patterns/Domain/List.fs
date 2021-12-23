module Patterns.Domain.List

type IngestionService internal (tip : ListTip.Service<_, _, _>) =

    member _.IngestItems(originEpochId, items : ItemId[]) =
        tip.IngestMany(originEpochId, items)

module Config =

    let createIngester_ linger maxItemsPerEpoch store =
        let series = ListSeries.Config.create store
        let epochs = ListEpoch.Config.create maxItemsPerEpoch store
        let tip = ListTip.create linger (series.ReadIngestionEpochId, series.MarkIngestionEpochId) (epochs.Ingest, Array.toSeq)
        IngestionService(tip)
    let createIngester store =
        let defaultLinger = System.TimeSpan.FromMilliseconds 200.
        let maxItemsPerEpoch = 10_000
        createIngester_ defaultLinger maxItemsPerEpoch store
