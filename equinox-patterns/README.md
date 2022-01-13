# Equinox Patterns

This template provides a grab-bag of example Deciders, including illustrations of the following generic techniques:

## `Period` with Rolling Balance carried forward

Consists of:

- `Period`: a Decider that manages ingestion into a chain of periods

  Each period within the sequence has the following lifecycle:

  1. Carrying Forward a Balance from its immediate predecessor `Period`; then
  2. being open for transactions for a period of time; ultimately
  3. carrying forward a closing balance to its successor Period
  
Notes:
- A given `Period` can thus be read without any need to load any preceding periods, as by definition, all relevant information has been `CarriedForward`
- Any `Period` is guaranteed to have been initialized; thus the preceding epochs can safely be archived the moment the first event has been written to a given `Period`

## `List` Epochs/Series with exactly once ingestion guarantee

Consists of:

- `ExactlyOnceIngester`: Generic algorithm that manages efficient deterministic ingestion into a chain of epochs in a series
- `ListEpoch`: Represents a span of activity during the life of the list. May be closed at an arbitrary point in time by any writer.
- `ListSeries`: Records the identifier of the current active Epoch of the series.
- `ListIngester`: Uses an `ExactlyOnceIngester` to insert at the tail of a chain of `ListEpoch`s indexed by a `ListSeries`

`ExactlyOnceIngester` accomplishes this by having each insertion logically:
  
  1. 'take a ticket':- grab the current epoch id from which we'll commence an attempt to insert into the list (the Ingester holds this memory for efficiency)
  2. Ensure this is stored at source in order to ensure that an idempotent reprocessing can _guarantee_ to traverse the epochs from the same starting point
  3. Coalesce concurrent requests in order that concurrent insertions do not result in optimistic concurrency violations when those writers all converge on the same stream in the underlying store

NOTE: the `feedSource` template illustrates a different ingestion scheme with different properties:-
- No requirement for the source to be able to log a starting epochId as necessary in the scheme implemented here
- Best-effort deduplication only (lookback is a defined number of batches, repeats outside that window are possible), on the assumption that the consumer will be able to deal with that cleanly
- Slightly more code, uses some memory to maintain the deduplication list

_In general, the scheme here is a better default approach unless you have specific performance requirements which dictate that the scheme in `feedSource` is the only one that's viable_
