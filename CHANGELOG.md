# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added
### Changed

- `dotnet new eqxprojector` now uses `Jet.ConfluentKafka.FSharp 1.0.0-rc1` (which uses `Confluent.Kafka 1.0.0-RC2`, `librdkafka 1.0.0`)
- Target `Equinox`.* v `2.0.0-preview3`
- `dotnet new eqxetl` now supports command-line category white/blacklist [#18](https://github.com/jet/dotnet-templates/pull/18)
- `dotnet new eqxetl` now supports command-line selection of an `aux` collection in either the `source` or destination collections [#18](https://github.com/jet/dotnet-templates/pull/18)

### Removed
### Fixed

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

[Unreleased]: https://github.com/jet/dotnet-templates/compare/2.0.0...HEAD
[2.0.0]: https://github.com/jet/dotnet-templates/compare/1.2.0...2.0.0
[1.2.0]: https://github.com/jet/dotnet-templates/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/jet/dotnet-templates/compare/1061b32ff1d86633e4adb0ce591992aea9c48c1e...1.1.1