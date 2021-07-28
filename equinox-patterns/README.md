# Equinox Patterns

This template provides a grab-bag of example Deciders, including illustrations of the following generic techniques:

## `Period` with Rolling Balance carried forward

Consists of:

- `Period`: a Decider that manages ingestion into a chain of periods with each one
  a) Carrying Forward a Balance from its immediate predecessor `Period`; then
  b) being open for transactions for a period of time; ultimately
  c) carrying forward a closing balance to its successor Period
  
Notes:
- A given `Period` can thus be read without any need to load any preceding periods, as by definition, all relevant information has been `CarriedForward`
- Any `Period` is guaranteed to have been initialized; thus the preceding epochs can safely be archived the moment the first event has been written to a given `Period`

## Epochs/Series with best effort (`LookbackIngester`) or exactly once (`ExactlyOnceIngester`) deduplication

Consists of:

- `ItemEpoch`: A given atomic set of items that have been ingested. May be closed at an arbitrary point in time by any writer.
- `ItemSeries`: Records the identifier of the current active Epoch of the series.

Notes:
- For `LookBackIngester`, /// - Ingestion deduplicates on a best-effort basis looking back a predefined number of epochs. Re-ingestion of items prior to this window is possible.

- 
- the deduplication is limited by, , can 100% guarantee exactly a single copy of the item will be stored in the series as a whole.
