# Copilot Instructions

## What This Repo Is

This repo hosts the source for [`dotnet new`](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-new) templates, published as the [`Equinox.Templates`](https://www.nuget.org/packages/Equinox.Templates) NuGet package. Each top-level directory (e.g. `equinox-web/`, `propulsion-projector/`) is a template whose content gets packed into the NuGet and installed via `dotnet new`.

Templates are primarily F# (except `equinox-web-csharp/` which is C#). Core libraries used across templates: [Equinox](https://github.com/jet/equinox), [Propulsion](https://github.com/jet/propulsion), [FsCodec](https://github.com/jet/FsCodec), Argu, Serilog.

## Build & Test

```bash
# Pack the NuGet package only
dotnet msbuild build.proj -t:Pack

# Pack then run all template tests (what CI runs)
dotnet test --project build.proj -v n

# Run tests directly (requires the .nupkg to already exist in bin/nupkg/)
dotnet test tests/Equinox.Templates.Tests --configuration Release

# Run a single test by name (test names match template shortName, e.g. eqxPatterns, proProjector)
dotnet test tests/Equinox.Templates.Tests --filter "FullyQualifiedName~eqxPatterns"
dotnet test tests/Equinox.Templates.Tests --filter "FullyQualifiedName~proProjector"

# Run a single test in DEBUG mode (unlocks the `*pending*` fact for ad-hoc testing)
dotnet test tests/Equinox.Templates.Tests --configuration Debug --filter "FullyQualifiedName~pending"
```

The test suite works by:
1. Installing the `.nupkg` from `bin/nupkg/` via `dotnet new -i`
2. Expanding each template into a `scratch-area/` subdirectory
3. Running `dotnet build` on the generated project to validate it compiles
4. Uninstalling the package on teardown

## Template Structure

Each template directory follows this layout:
```
<template-shortname>/
  .template.config/template.json   ← dotnet templating metadata + parameter definitions
  *.fsproj (or *.csproj)
  *.fs source files
  README.md
```

The `src/Equinox.Templates/Equinox.Templates.fsproj` packs all `equinox-*`, `propulsion-*`, `feed-*`, and `periodic-*` directories as NuGet content.

## Conditional Template Content (`#if` Directives)

Template source files use C preprocessor-style directives for conditional code inclusion based on template parameters declared in `template.json`. Parameters become symbols that gate code blocks:

In `.fs` files, standard F# preprocessor syntax is used:
```fsharp
#if kafka
    | [<AltCommandLine "-b"; Unique>] Broker of string
#endif

// #if cosmos      ← commented-out means always included; cosmos block is default-on
    | Cosmos of ...
// #endif
```

In `.fsproj` files, XML comment syntax is used instead:
```xml
<!--#if (esdb)-->
<Compile Include="SourceArgs.Esdb.fs" />
<!--#endif-->
<!--#if kafka-->
<PackageReference Include="Propulsion.Kafka" Version="..." />
<!--#endif-->
```

When adding a new parameter to a template: declare it in `template.json` under `symbols`, then use `#if`/`#endif` in the `.fs`/`.fsproj` files. Computed symbols (derived from choice parameters) use `"type": "computed"` with a `"value"` expression. File inclusion/exclusion uses `modifiers` in `template.json`.

## Template Short Names

| Short name          | Template directory          | Type     |
|---------------------|-----------------------------|----------|
| `eqxweb`            | `equinox-web/`              | solution |
| `eqxwebcs`          | `equinox-web-csharp/`       | solution |
| `eqxPatterns`       | `equinox-patterns/`         | solution |
| `eqxTestbed`        | `equinox-testbed/`          | solution |
| `eqxShipping`       | `equinox-shipping/`         | solution |
| `proProjector`      | `propulsion-projector/`     | project  |
| `proConsumer`       | `propulsion-consumer/`      | project  |
| `proReactor`        | `propulsion-reactor/`       | project  |
| `proSync`           | `propulsion-sync/`          | project  |
| `proArchiver`       | `propulsion-archiver/`      | project  |
| `proPruner`         | `propulsion-pruner/`        | project  |
| `proIndexer`        | `propulsion-indexer/`       | project  |
| `proDynamoStoreCdk` | `propulsion-dynamostore-cdk/` | project |
| `proHotel`          | `propulsion-hotel/`         | project  |
| `feedSource`        | `feed-source/`              | project  |
| `feedConsumer`      | `feed-consumer/`            | project  |
| `periodicIngester`  | `periodic-ingester/`        | project  |
| `trackingConsumer`  | `propulsion-tracking-consumer/` | project |
| `summaryConsumer`   | `propulsion-summary-consumer/` | project |

## Versioning

Versions are derived automatically from git tags via [MinVer](https://github.com/adamralph/minver). No manual version bumps needed. PR builds get a `-pr.<number>` suffix via the `BUILD_PR` environment variable.

## Key Conventions

- **Target framework**: `net8.0` for EXE projects; `netstandard2.0` for the template package itself.
- **Test variants**: `DotnetBuild.fs` uses xUnit `TheoryData` classes (e.g. `ProProjector`, `EqxWebs`) to enumerate parameter combinations. DEBUG builds run a reduced subset; Release runs the full matrix.
- **`scratch-area/`**: Generated by tests; do not commit. Cleared before each test run.
- **`bin/nupkg/`**: Output of pack; do not commit.
- **FS2003 / NU5105** warnings are suppressed globally in `Directory.Build.props` — these are expected false positives.
- **FS3579** (`WarnOn`) is enabled to catch untyped string interpolations.
- Template project files set `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>` and `<WarningLevel>5</WarningLevel>`.
