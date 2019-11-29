# Equinox FC Sample

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install in the local templates store

    dotnet new eqxfc # use --help to see options regarding storage subsystem configuration etc

# `Location*`

The `Location`* `module`s illustrates a way to approach the modelling of a long-running state by having writers adhere to a common protocol when writing:

- the `LocationEpoch` category represents a span of time, which is guaranteed to have a `CarriedForward` event representing the opening balance, and/or the balance carried forward when the preceding epoch was marked `Closed`
- the `LocationSeries` category bears the verified active epoch (competing readers/writers read this optimistically on a cached basis; in the event that they're behind, they'll compete to log the successful commencement of a new epoch, which cannot happen before the `CarriedForward` event for the successor epoch has been committed)

- `Domain.Tests` includes (`FsCheck`) Property-based unit tests, and an integration tests that demonstrates parallel writes (that trigger Optimistic Concurrency Control-based conflict resolution, including against `MemoryStore`)

## Notes

- Referencing an `Equinox.*` Store module from the `Domain` project is not mandatory; it's common to defer all wiring and configuration of the elements in `module Cosmos`, `module EventStore` etc. and instead maintain that alongside the Composition Root, outside of the `Domain` project

- While using an `AccessStrategy` such as `Snapshotting` may in some cases be relevant too, in the general case, using the `Equinox.Cache`, combined with having a compact Fold `State` and a sufficiently constrained maximum number/size of events means the state can be established within a predictable latency range.

- Representing a long-running state in this fashion is no panacea; in modeling a system, the ideal is to have streams that have a naturally constrained number of events over their lifetime.

- [Original PR](https://github.com/jet/dotnet-templates/pull/40)