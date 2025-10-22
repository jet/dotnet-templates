# Indexer App

The purpose of the Indexer app is to:
- consume the Main Container's ChangeFeed (container name is configured via `EQUINOX_COSMOS_CONTAINER`)
- ensure all derived information maintained in the Views Container is kept in sync (`EQUINOX_COSMOS_VIEWS`)

It also implements some additional functions (all of which are based on the consumption of Equinox events from the Main Container via the ChangeFeed).

# Command: `index`

:warning: **Writes to the Views Container; Does not write to the Main Store Container**.

Reacts to each event written to the main store by updating all derived information held in the Views Container.

From the base of the repo, it's invoked as follows:

    dotnet run --project Indexer -- -g defaultIndexer index

- `defaultIndexer` is the Processor Name (equivalent to the notion of a Consumer Group in many messaging technologies)

## Common commandline overrides

The following overrides can be useful:

- `-w` overrides the streams parallelism limit (default: `8`). In general, Cosmos rate limiting policy and request latency means increasing beyond that is not normally useful. A potential exception is when retraversing due to the RU consumption from writes being avoided (i.e. correct efficient idempotent processing).
- `-r` overrides the maximum read-ahead limit (default: `2`).
  - In rare cases (where the I/O involved in processing events is efficient and/or the items are being traversed at a high throughput) increasing the read ahead limit can help to ensure that there's always something for the processor 'threads' to work on
  - reading ahead can help throughput in some cases: multiple events from the same stream are handled as a single span of events to the maximum degree possible, including events from batches beyond the one that's currently in progress.

There are other settings that relate to the source ChangeFeed (i.e. you specify them after the `index` subcommand name):

-  `-b`: adjusts the maximum batch size (default: `100` items).
   - Lower values induce more frequent checkpointing, and reduce the RU cost of each individual read.
   - Higher values increase the RU cost of each batch read and the number of events that need to be re-processed if a partition is re-assigned, or the Index App is restarted.

# Command: `snapshot`

:warning: **Writes to the Main Store Container; Does not write to the Views Container**.

This ancillary command is hosted within the Indexer App as it involves the same elements as the normal indexing behavior (reading from the ChangeFeed, using the domain fold logic to establish the state per stream, writing to the store as configured).

The key difference compared to the `index` subcommand is that it writes to the Main Store Container (as opposed to the Views Container).

From the base of the repo, it's invoked as follows:

    dotnet run --project Indexer -- -g defaultSnapshotter snapshot

- `defaultSnapshotter` is the _Processor Name_ (aka Consumer Group Name).
  - It should be different to the one used for the Indexing behavior (`defaultIndexer` in the example above).
  - To trigger full retraversal (as opposed to only visiting streams that have been updated since the last `snapshot` traversal, you'll want to use a fresh checkpoint name per run, see [resetting checkpoints](https://github.com/jet/propulsion/blob/doc/DOCUMENTATION.md#resetting-checkpoints))

## Implementation notes

Replaces the normal/default Handler with one that:
1. **loads the State of a stream** with the normal application-level `fold` logic appropriate for its category, noting whether the most recent Snapshot Event Type was present. Having completed the load, it does one of the following:
    - (if the state was derived from the latest Snapshot Event Type): _no further action is necessary_
    - (if no snapshot is present) Equinox builds the state in the normal manner by `fold`ing all the events
2. **(if the state was loaded based on the snapshot)**:
    - no writes take place
    - the Equinox Cache will retain the version and state (and via step `4` below, become Propulsion's updated 'write position' for that stream)
    - subsequent reads of events from the ChangeFeed will typically be discarded (as the `Index` will be less than the `Version` that became that stream's write position)

   :bulb: **NOTE:** if a fresh event enters the store after the stream has been visited in a given traversal, but the writer did not write a 'latest' snapshot alongside, the snapshotter would react by immediately adding the snapshot. (For avoidance of doubt, its extremely unlikely that this will work out better in terms of RU consumption and/or latency, as the RU impact of a write is a function of the original size plus the updated size)

   :bulb: **ASIDE:** If a [purge](https://github.com/jet/propulsion/blob/doc/DOCUMENTATION.md#purging) causes the write position to be jettisoned, the cache entry comes into play: the etag in the cached state will likely result in a 304 not modified read roundtrip
3. **(if the state was not derived from the Snapshot Event Type)**
    - yields a `Snapshotted` event from the `decide` function (to trigger a store Sync operation rather than the normal short-circuit if there are no events to write)
    - use `AccessStrategy.Custom` to `transmute` this tentative write:
        - _from_: the proposed snapshot event that the `decide` function yielded in the preceding step to trigger the store roundtrip (which would ordinarily we written as part of the streams permanent events, which would be bad)
        - _to_: not appending any events, but instead replacing the 0 or many current unfolds with the single desired snapshot

    :bulb: **NOTE:** the `CosmosStoreContext` used to do the processing is configured differently to how one normally would for this to work well; `tipMaxEvents` is set to a number that's effectively infinite:

      - e.g. if you set it to `20` and tried to update the snapshot of a stream with 30 events, a 'calve' would be triggered, resulting in:

        1. writing a calf Item containing the `30` ejected events
        2. updating the Tip to not have any `e`vents
        3. updating the Tip to have the updated `Snapshotted` event as the single entry in the `u`nfolds field
4. yields the Next Index Position based on the observed version of the stream.

  :bulb: Normally, the first visit to the stream is the only one. The exception is if a new write takes place before the processing reaches the end of the ChangeFeed.

In other aspects (from the perspective of how it traverses the events, checkpoints progress, manages concurrency and/or reading ahead), this subcommand is equivalent to the  `index` subcommand; the same overrides apply.

# Command: `sync`

:warning: **Writes to the Main Store Container; Does not write to the Views Container**.

This ancillary command is similar to the `proSync` template and the `propulsion sync` tool command.

It is included within the Indexer App as it involves the same elements as the normal indexing behavior (reading from the ChangeFeed, writing to the store as configured).

The key difference compared to the `export` subcommand is that it writes to the Main Store Container (as opposed to a nominated `export cosmos` destination container).

From the base of the repo, it's invoked as follows:

    dotnet run --project Indexer -- -g defaultSync sync cosmos -d DevDatabase -c DevStore`

- `defaultSync` is the _Processor Name_ (aka Consumer Group Name).
    - It should be different to the one used for the Indexing behavior (`defaultIndexer` in the example above).
    - To trigger full retraversal (as opposed to only visiting streams that have been updated since the last `snapshot` traversal, you'll want to use a fresh checkpoint name per run, see [resetting checkpoints](https://github.com/jet/propulsion/blob/doc/DOCUMENTATION.md#resetting-checkpoints))
