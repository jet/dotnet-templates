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

----

# Indexing logic fundamentals

Over time, the number of events in the system will keep growing. But, you still want to be able to reconstitute the content and/or add a new variant at any time.

This has the following implications:
- processing should work on a batched basis
- no permanent state should be held in the Views Container; it's literally just a structure that's built up to support the current needs of the system as a whole in terms of providing for efficient traversal of data to support the use cases and performance needs of both read and write paths.

## Key principle: The views Container is ephemeral

In a deployed instance, indexing typically runs as live loop working from the tail of the ChangeFeed.

However, it's important to note that in a well written event-sourced system that it should be considered absolutely normal to, at the drop of a hat be able to regenerate the views and/or provision a slightly tweaked version of the existing format.

It is easy, and should remain easy to validate that a given indexing function works correctly and efficiently: blow away the views container and retraverse on the desktop or in a sandbox environment.

:bulb: NOTE this is one reason why using Kafka or some form of Service Bus to consume events indirectly is problematic; you need to re-emit all the events to the 'bus'

## Example: Completely regenerating the Views Container

The only real 'state' in the system as a whole is the ChangeFeed processor checkpoint (identified by the Processor Name in the `-g` commandline argument)

If there's ever doubt about the integrity of the state in the Views Container, it should be possible to reconstitute its content at any time by:

1. delete the Views Container
2. re-initialize it with `eqx init`
3. run `index` over all the events from the start of time

## Example: capturing fields from an event into the View Container state that were not previously captured

In general, Indexes should not speculatively hold information that's not being used - writing more than is necessary consumes space and increases RU consumption.

Instead, where a new feature and/or a tweak to an existing one requires previously un-indexed information, one can simply use an Expand and Contract strategy:
1. Expand to add the extra information
   - change the `Category` for the view (i.e. if it was `$TodoIndex`, and a new field is needed, change the `Category` to `$TodoIndex2`)
   - add the logic to populate the relevant state
   - run the `index` subcommand (with a fresh _Processor Name_)
2. Write consumer logic
3. Deploy. This can be done in two common ways:
   - (if the consumption logic is not behind a feature flag) as part of the deploy, run the `index` before the system goes live
   - (if the consumption logic is behind a feature flag):
     - after the deploy, run an `index` operation
     - when the index has caught up, enable the feature flag that uses the information it provides

----

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
4. yields a `StreamResult.OverrideNextIndex` based on the observed version of the stream.

  :bulb: Normally, the first visit to the stream is the only one. The exception is if a new write takes place before the processing reaches the end of the ChangeFeed.

In other aspects (from the perspective of how it traverses the events, checkpoints progress, manages concurrency and/or reading ahead), this subcommand is equivalent to the  `index` subcommand; the same overrides apply.

## Versioning events

All types used in the body of an event need to be able to be safely round-tripped to/from the Event Store. While events are written exactly once, they will live in the store forever, so need to be designed carefully.

As a result, you want to follow the normal principles of doing changes to message contracts safely:
- the most obvious thing not to do is rename fields
- removing fields (as long as you know no reader anywhere is going to have an exception in their absence)
- adding new fields as `option`s (assuming you can do something workable in the case where the value is `None)

That said, the most important thing in defining the contract for a normal event sourced event is the semantic intent; if the intent/meaning behind an event is changing, it's normally best to add a new case to the `Event` union to reflect that.

:bulb: Whenever an Event is no longer relevant to the system as a whole, it's safe to remove it from the `type Event` union - Event Types that don't have cases in the union are simply ignored.

There's a wide variety of techniques and established strategies, most of which are covered in https://github.com/jet/fscodec#upconversion

## Versioning snapshots

The `Snapshotted` event in the `type Event =` union is explicitly tagged with a `Name`:

```fsharp
| [<DataMember(Name = "Snapshotted")>] Snapshotted of Snapshotted
```

As with any Event, all types used in the body (i.e. the entire tree of types used in the `type Snapshotted` needs to be able to be roundtripped safely to/from the Store).

The same rules above re Versioning Events apply equally.

The critical differences between 'real Events' and `Snapshotted` events are:
- Snapshotted event are stored in the `u`nfolds field of the tip, and get replaced with a fresh version every time an event is updated
- It's always theoretically safe to remove the `Snapshotted` case from the Union; no data would be lost. However it does mean that the state will need to be produced from the events, which will likely result in higher latency and RU consumption on the initial load (the Cache means you generally only pay this price once).

The following general rules apply:

- CONSIDER per aggregate whether a `Snapshotted` event is appropriate. It may make sense to avoid having a snapshotted stream if streams are short, and you are using Events In Tip mode
  - the cache will hold the state, and reads and writes don't pay the cost of conveying a snapshot
  - the additional cost of deserializing and folding the events is extremely unlikely to be relevant when considering the overall I/O costs
- ALWAYS change the `Name` _string_ from `Snapshotted` to `Snapshotted/2`, `Snapshotted/3` etc whenever a breaking change is needed - that removes any risk of a failure to parse the snapshot event body due to changes in the representation.

Given the above arrangements, you can at any time ensure snapshots have been updated in a given environment by running the following command against the Store: 

  ```bash
  # establish prod env vars for EQUINOX_COSMOS_CONNECTION, EQUINOX_COSMOS_DATABASE, EQUINOX_COSMOS_CONTAINER etc
  dotnet run --project Indexer -- -g deployYYMMDD snapshot
  ```
----

# Command: `sync`

:warning: **Writes to the nominated 'Export' Container; Does not write to the `source` (or the Views Container)**.

Uses the ChangeFeed to export all the events from the Main Store Container to a nominated target Equinox Container.

This ancillary command is hosted within the Indexer App despite involving less of the infrastructure required for the `index` or `snapshot` subcommands as it is commonly used in conjunction with those other subcommands.

In addition to being useful on a one-off basis, it can also be run to maintain a constantly in sync read replica of a given Container.

From the base of the repo, it's invoked as follows:

    eqx init -A cosmos -c Export # once; make an empty container to Sync into
    dotnet run --project Indexer -g syncExport sync -c Export source
    eqx stats -A cosmos # get stats for Main store
    eqx stats -A cosmos -c Export # compare stats for exported version

- `syncExport` is a _Processor Name_ (aka Consumer Group Name) that is different to the one used for the Indexing behavior (`defaultIndexer` in the example above)
- `source` is a mandatory subcommand of the `sync` command; you can supply the typical arguments to control the ChangeFeed as one would for `index` or `snapshot`

:bulb: **NOTE** The Sync process only writes events. Each write will replace any snapshots stored in the `u`nfolds space in the Tip with an empty list. This means your next step after 'exporting' will frequently be to point the `EQUINOX_COSMOS_CONTAINER` at the exported version and then run the `snapshot` subcommand (while the system will work without doing so, each initial load will be less efficient).

:bulb: **ASIDE:** You could run the `sync` concurrently with the `snapshot`; the outcome would be correct, but it would typically be inefficient as they'd be duelling for the same RU capacity. Also, depending on whether or not the source events are stored compactly (using Events In Tip), the streams may be written to many times in the course of the export (and each of those appends would mark that stream 'dirty' and hence trigger another visit by the `snapshot` processing)

## Example: exporting to a local database from a production environment

```bash
EQUINOX_COSMOS_CONNECTION=<connection string>
EQUINOX_COSMOS_DATABASE=testdb
EQUINOX_COSMOS_CONTAINER=app # note we override this in most cases below with `-c test`
EQUINOX_COSMOS_VIEWS=app-views
SOURCE_COSMOS_KEY=<copy read only key from portal>
propulsion init -A cosmos -c app # creates app-aux
eqx init -A cosmos -c app-views
eqx init -A cosmos -c test
# copy from prod datastore into a temporary (`test`)
dotnet run --project Indexer -- -w 64 -g defaultSync `
    sync -s $EQUINOX_COSMOS_CONNECTION -c test -a app-aux `
    source -s $SOURCE_COSMOS_KEY -d productiondb -c app -b 1000
# apply snapshots to the exported database (optional; things will still work without them)
dotnet run --project Indexer -- -g defaultSnapshotter `
    snapshot -c test -a app-aux # note need to specify app-aux, or it would look for test-aux
# index the data into the app-views store based on the test export
dotnet run --project Indexer -- -g defaultIndexer `
    index -c test -a app-aux -b 9999
```

- `-w 64`: override normal concurrency of 8
- `-b 9999`: reduce ChangeFeed Reader messages
