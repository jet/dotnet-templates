# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added
### Changed

- Target `Equinox`.* v `2.0.0-rc9`, `Propulsion`.* v `2.0.0-rc1`, `FsCodec`.* v `2.0.0-rc3`
- Standardize Aggregate layouts
- Apply encapsulation techniques from https://github.com/jet/FsCodec#decoding-events in consumers [#43](https://github.com/jet/dotnet-templates/pull/43)

### Removed
### Fixed

<a name="3.8.0"></a>
## [3.8.0] - 2019-12-07

### Changed

- Target Propulsion 1.5.0, FsKafka 1.3.0
- Use uppercase for switch arguments in commandline parsing

### Fixed

- Included stacktrace in Exception-exit logging

<a name="3.7.0"></a>
## [3.7.0] - 2019-11-14

### Changed

- Target Propulsion 1.4.0, Equinox 2.0.0-rc8, FsCodec 1.2.1

<a name="3.6.1"></a>
## [3.6.1] - 2019-11-13

### Added

- Split out Settings initialization [#34](https://github.com/jet/dotnet-templates/pull/34)

### Changed

- `summaryConsumer`: Target FsCodec 1.1.0 to simplify `up` function [#32](https://github.com/jet/dotnet-templates/pull/32)
- `trackingConsumer`: switch serializer to FsCodec [#35](https://github.com/jet/dotnet-templates/pull/35)
- use `-g` for ES and Cosmos `ConsumerGroupName` values [#37](https://github.com/jet/dotnet-templates/pull/37)
- `summaryProjector`: Use `AllowStale` for CheckpointSeries as should only typically run single instance
- Removed anonymous records usage to support usage with v `2.1.504` SDK

<a name="3.6.0"></a>
## [3.6.0] - 2019-10-17

### Changed

- Target Propulsion 1.3.0, Equinox 2.0.0-rc7, FsCodec 1.0.0

<a name="3.5.0"></a>
## [3.5.0] - 2019-10-11

### Added

- EventStore source support for `summaryProjector` [#31](https://github.com/jet/dotnet-templates/pull/31)

### Changed

- Target Propulsion 1.2.1 (minor change to accommodate signature change in .EventStore checkpoints)
- Lots of consistency + layout work resulting from porting work

<a name="3.4.1"></a>
## [3.4.1] - 2019-10-05

### Changed

- Naming consistency and generalizations extracted from porting work.

<a name="3.4.0"></a>
## [3.4.0] - 2019-09-18

### Added

- `trackingConsumer` template providing a baseline for projecting accumulating updates across autonomous bounded contexts: [#30](https://github.com/jet/dotnet-templates/pull/30) [@luo4neck](https://github.com/luo4neck)

### Changed

- Significant reformatting and consistency updates in preparation for adding ES support for `summaryProjector` in [#31](https://github.com/jet/dotnet-templates/pull/31)

<a name="3.3.2"></a>
## [3.3.2] - 2019-09-15

### Changed

- Target Propulsion 1.2.0 (minor breaking change in source due to parameter reordering)

<<a name="3.3.1"></a>
## [3.3.1] - 2019-09-08

### Changed

- Target FsCodec 1.0.0-rc2, Equinox 2.0.0-rc6, Propulsion 1.0.2-alpha.0.1

<a name="3.3.0"></a>
## [3.3.0] - 2019-09-03

### Added

- `summaryProjector`, `summaryConsumer` templates providing a baseline for projecting summaries between autonomous bounded contexts: [#29](https://github.com/jet/dotnet-templates/pull/29) [@fnipo](https://github.com/fnipo)

#<a name="3.2.0"></a>
## [3.2.0] - 2019-08-31

### Changed

- Target `Propulsion`.* `1.0.1`, `Equinox`.* `2.0.0-rc4` (handling name changes, esp wrt Collection->Container terminology) [#28](https://github.com/jet/dotnet-templates/pull/28)
- Target `Propulsion`.* `1.1.0`, `Equinox`.* `2.0.0-rc5`, `FsCodec`.* `1.0.0-rc1`, simplifying Codec logic

### Fixed

- Removed various over-complex elements of samples

<a name="3.1.0"></a>
## [3.1.0] - 2019-07-05

### Added

- `propulsion-sync`: Support emission of select events direct to Kafka [#26](https://github.com/jet/dotnet-templates/pull/26)

### Changed

- Target `Propulsion`.* `1.0.1-rc8`
- `EQUINOX_KAFKA_`* -> `PROPULSION_KAFKA_`*

### Fixed

- Removed reliance on `IEnumerable<IEvent>` in `RenderedSpan` and `StreamSpan`

<a name="3.0.3"></a>
## [3.0.3] - 2019-07-02

### Added

- Add `Publisher.fs` sample to `proConsumer`
- Simplify `proConsumer` `Examples.fs`

### Changed

- Target `Equinox`.* `2.0.0-rc2`, `Propulsion`.* `1.0.1-rc5`

<a name="3.0.2"></a>
## [3.0.2] - 2019-06-19

### Added

- `proSync` has EventStore Sink support via `cosmos` ... `es` commandline option [#23](https://github.com/jet/dotnet-templates/pull/23)
- `proSync` has EventStore Source support via `es` ... `cosmos` commandline option [#16](https://github.com/jet/dotnet-templates/pull/16)

- `proConsumer` offers a `StreamSpan`-based API for ordered, de-deduplicated consumption without concurrent executions at stream level [#24](https://github.com/jet/dotnet-templates/pull/24)
- `proConsumer` summarizes processing outcomes in its examples using new support for same in `Propulsion.Kafka` [#25](https://github.com/jet/dotnet-templates/pull/25)
- `proConsumer -n`'s offers a parallel mode that runs all projections in parallel without constraints (or need to synthesize streams) [#24](https://github.com/jet/dotnet-templates/pull/24)

### Changed

- `eqxsync` renamed to `proSync`
- `eqxProjector` split to `proProjector` and `proConsumer`
- `eqxtestbed`, `eqxweb`, `eqxwebcs` now target `Equinox 2.0.0-rc1`
- `proConsumer`, `proProjector -k` now target `Jet.ConfluentKafka.FSharp` + `Propulsion.Kafka` v `1.0.1-rc3` [#24](https://github.com/jet/dotnet-templates/pull/24)
- `proSync` now targets `Propulsion.Cosmos`,`Propulsion.EventStore` v `1.0.1-rc3` [#24](https://github.com/jet/dotnet-templates/pull/24)

<a name="2.2.2"></a>
## [2.2.2] - 2019-05-17

### Added

- `dotnet new eqxprojector` uses separated read/write/progress pipeline [#22](https://github.com/jet/dotnet-templates/pull/22)

### Changed

- `dotnet new eqxprojector -k` now targets `Jet.ConfluentKafka.FSharp 1.0.0-rc7` (which targets `Confluent.Kafka 1.0.0`, `librdkafka 1.0.0`)
- `dotnet new eqxsync` uses separated read/write/progress pipeline [#21](https://github.com/jet/dotnet-templates/pull/21)
- targets `Equinox 2.0.0-preview8`

### Fixed

- `dotnet new eqxprojector` correctly handles progress writing [#22](https://github.com/jet/dotnet-templates/pull/22)

<a name="2.1.2"></a>
## [2.1.2] - 2019-04-15

### Added

- `dotnet new eqxsync` has separated processing for progress computation, progress writing and batch loading (this also happens to be the only way in which to balance throughput with correctness in the context of a ChangeFeedProcessor) [#19](https://github.com/jet/dotnet-templates/pull/19)
- `dotnet new eqxsync` separates out notion of the `CosmosIngester` and `ProgressBatcher` and their respective tests [#20](https://github.com/jet/dotnet-templates/pull/20)

### Changed

- `dotnet new eqxetl` is now `dotnet new eqxsync`
- `dotnet new eqxsync` now supports command-line category white/blacklist [#18](https://github.com/jet/dotnet-templates/pull/18)
- `dotnet new eqxsync` now supports command-line selection of an `aux` collection in either the `source` or destination collections [#18](https://github.com/jet/dotnet-templates/pull/18)
- targets `Equinox`.* v `2.0.0-preview5`
- `dotnet new eqxprojector` now targets `Jet.ConfluentKafka.FSharp 1.0.0-rc3` (which targets `Confluent.Kafka 1.0.0-RC4`, `librdkafka 1.0.0`)

<a name="2.0.0"></a>
## [2.0.0] - 2019-03-26

### Added

- `dotnet new eqxprojector` template, providing a CosmosDb `ChangeFeedProcessor` host app, with or without a Kafka Producer and Kafka Consumer host app using [the `Jet.ConfluentKafka.FSharp` wrapper for `Confluent.Kafka` v `1.0.0-beta3`](https://github.com/jet/Jet.ConfluentKafka.FSharp/tree/v1) [#11](https://github.com/jet/dotnet-templates/pull/11)
- `dotnet new eqxtestbed` template, providing a host that allows running back-to-back benchmarks when prototyping models, using different stores and/or store configuration parameters [#14](https://github.com/jet/dotnet-templates/pull/14)
- `dotnet new eqxetl` template, providing a CosmosDb `ChangeFeedProcessor` that ingests/transforms/filters documents from a source store, feeding events (consistently) into an `Equinox.Cosmos` store [#17](https://github.com/jet/dotnet-templates/pull/17)

### Changed

- `dotnet new eqxweb` now uses Anonymous Records syntax HT [@ameier38](https://github.com/ameier38)
- `dotnet new eqxprojector` now uses `Jet.ConfluentKafka.FSharp 1.0.0-preview2` (which uses `Confluent.Kafka 1.0.0-RC1`)

<a name="1.2.0"></a>
## [1.2.0] - 2019-02-06

### Changed

- `dotnet new eqxweb` now uses FSharp.UMX to make Id types more succinct [#12](https://github.com/jet/dotnet-templates/pull/12)
- Target Equinox 1.0.4-rc1, which entails minor source changes to both C# and F# [#12](https://github.com/jet/dotnet-templates/pull/12)

### Fixed

- Fix project type guids to render C# projects (was showing F# logo) @aarondandy [#8](https://github.com/jet/dotnet-templates/pull/8)

<a name="1.1.1"></a>
## [1.1.1] - 2019-01-17

### Added

- C# port of template - `dotnet new eqxwebcs`, prompting cleanup work in [jet/equinox#81](https://github.com/jet/equinox/pull/81) [#5](https://github.com/jet/dotnet-templates/pull/5)

  Thanks to @aarondandy for early legwork

  Thanks to @mcintyre321 for excellent direct and indirect suggestions regarding how to tidy the Domain implementations

### Changed

- F# template has been renamed to: eqxweb (was `equinoxweb`)

(For information pertaining to earlier releases, see release notes in https://github.com/jet/dotnet-templates/releases and/or can someone please add it!)

[Unreleased]: https://github.com/jet/dotnet-templates/compare/3.8.0...HEAD
[3.8.0]: https://github.com/jet/dotnet-templates/compare/3.7.0...3.8.0
[3.7.0]: https://github.com/jet/dotnet-templates/compare/3.6.1...3.7.0
[3.6.1]: https://github.com/jet/dotnet-templates/compare/3.6.0...3.6.1
[3.6.0]: https://github.com/jet/dotnet-templates/compare/3.5.0...3.6.0
[3.5.0]: https://github.com/jet/dotnet-templates/compare/3.4.1...3.5.0
[3.4.1]: https://github.com/jet/dotnet-templates/compare/3.4.0...3.4.1
[3.4.0]: https://github.com/jet/dotnet-templates/compare/3.3.2...3.4.0
[3.3.2]: https://github.com/jet/dotnet-templates/compare/3.3.1...3.3.2
[3.3.1]: https://github.com/jet/dotnet-templates/compare/3.3.0...3.3.1
[3.3.0]: https://github.com/jet/dotnet-templates/compare/3.2.0...3.3.0
[3.2.0]: https://github.com/jet/dotnet-templates/compare/3.1.0...3.2.0
[3.1.0]: https://github.com/jet/dotnet-templates/compare/3.0.3...3.1.0
[3.0.3]: https://github.com/jet/dotnet-templates/compare/3.0.2...3.0.3
[3.0.2]: https://github.com/jet/dotnet-templates/compare/2.2.2...3.0.2
[2.2.2]: https://github.com/jet/dotnet-templates/compare/2.1.2...2.2.2
[2.1.2]: https://github.com/jet/dotnet-templates/compare/2.0.0...2.1.2
[2.0.0]: https://github.com/jet/dotnet-templates/compare/1.2.0...2.0.0
[1.2.0]: https://github.com/jet/dotnet-templates/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/jet/dotnet-templates/compare/1061b32ff1d86633e4adb0ce591992aea9c48c1e...1.1.1