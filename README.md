# Jet `dotnet new` Templates [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.dotnet-templates?branchName=master)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=9?branchName=master) [![release](https://img.shields.io/github/release/jet/dotnet-templates.svg)](https://github.com/jet/dotnet-templates/releases) [![NuGet](https://img.shields.io/nuget/vpre/Equinox.Templates.svg?logo=nuget)](https://www.nuget.org/packages/Equinox.Templates) [![license](https://img.shields.io/github/license/jet/dotnet-templates.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/dotnet-templates.svg) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=slack">](https://t.co/MRxpx0rLH2)

This repo hosts the source for Jet's [`dotnet new`](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-new) templates.

## [Equinox](https://github.com/jet/equinox) related

- [`eqxweb`](equinox-web/README.md) - Boilerplate for an ASP .NET Core 2 Web App, with an associated storage-independent Domain project using [Equinox](https://github.com/jet/equinox).
- [`eqxwebcs`](equinox-web-csharp/README.md) - Boilerplate for an ASP .NET Core 2 Web App, with an associated storage-independent Domain project using [Equinox](https://github.com/jet/equinox), _ported to C#_.
- [`eqxtestbed`](equinox-testbed/README.md) - Host that allows running back-to-back benchmarks when prototyping models using [Equinox](https://github.com/jet/equinox), using different stores and/or store configuration parameters.

## [Propulsion](https://github.com/jet/propulsion) related

- [`proProjector`](propulsion-projector/README.md) - Boilerplate for an Azure CosmosDb ChangeFeedProcessor (typically unrolling events from `Equinox.Cosmos` stores using `Propulsion.Cosmos`)

  `-k` adds Optional projection to Apache Kafka using [`Propulsion.Kafka`](https://github.com/jet/propulsion).
  `-p` shows parallel consumption mode (where source is not stream-oriented; i.e. is not from `Equinox.Cosmos`)

- [`proConsumer`](propulsion-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion). (typically consuming from an app produced with `dotnet new proProjector -k`)

## Templates combining usage of Equinox and Propulsion

- [`proAllProjector`](propulsion-all-projector/README.md) - Boilerplate for an EventStore `$all` stream projector (projecting from an EventStore using `Propulsion.EventStore`.EventStore

  **NOTE At present, checkpoint storage is only implemented for Azure CosmosDB - help wanted ;)**

   Standard processing shows importing (in summary form) from `EventStore` to `Cosmos` (use `-b` to remove, yielding a minimal projector)
   
   `-k` adds Optional projection to Apache Kafka using [`Propulsion.Kafka`](https://github.com/jet/propulsion).
  
- [`summaryProjector`](propulsion-summary-projector/README.md) - Boilerplate for an a Projector that can consume from a) Azure CosmosDb ChangeFeedProcessor b) EventStore generating versioned [Summary Event](http://verraes.net/2019/05/patterns-for-decoupling-distsys-summary-event/) feed from an `Equinox.Cosmos`/`.EventStore` store using `Propulsion.Cosmos`/`.EventStore`.

- [`summaryConsumer`](propulsion-summary-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) to ingest versioned summaries produced by a `dotnet new summaryProjector`)

- [`trackingConsumer`](propulsion-tracking-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) to ingest accumulating changes in an `Equinox.Cosmos` store idempotently.

- [`proSync`](propulsion-sync/README.md) - Boilerplate for a console app that that syncs events between [`Equinox.Cosmos` and `Equinox.EventStore` stores](https://github.com/jet/equinox) using the [relevant `Propulsion`.* libraries](https://github.com/jet/propulsion), filtering/enriching/mapping Events as necessary.

## Walkthrough

As dictated by [the design of dotnet's templating mechanism](https://github.com/dotnet/templating/), consumption is ultimately via the .NET Core SDK's `dotnet new` CLI facility and/or associated facilities in Visual Studio, Rider etc.

To use from the command line, the outline is:
  1. Install a template locally (use `dotnet new --list` to view your current list)
  2. Use `dotnet new` to expand the template in a given directory

    # install the templates into `dotnet new`s list of available templates so it can be picked up by
    # `dotnet new`, Rider, Visual Studio etc.
    dotnet new -i Equinox.Templates

    # --help shows the options including wiring for storage subsystems,
    # -t includes an example Domain, Handler, Service and Controller to test from app to storage subsystem
    dotnet new eqxweb -t --help

    # if you want to see a C# equivalent:
    dotnet new eqxwebcs -t

    # see readme.md in the generated code for further instructions regarding the TodoBackend the above -t switch above triggers the inclusion of
    start readme.md

    # ... to add a Projector
    md -p ../Projector | Set-Location
    # (-k emits to Kafka and hence implies having a Consumer)
    dotnet new proProjector -k
    start README.md

    # ... to add a Consumer (proProjector -k emits to Kafka and hence implies having a Consumer)
    md -p ../Consumer | Set-Location
    dotnet new proConsumer
    start README.md

    # ... to add a Summary Projector
    md -p ../SummaryProjector | Set-Location
    dotnet new summaryProjector
    start README.md

    # ... to add a Summary Consumer (ingesting output from `summaryProjector`)
    md -p ../SummaryConsumer | Set-Location
    dotnet new summaryConsumer
    start README.md

    # ... to add a Testbed
    md -p ../My.Tools.Testbed | Set-Location
    # -e -c # add EventStore and CosmosDb suppport to got with the default support for MemoryStore
    dotnet new eqxtestbed -c -e
    start README.md
    # run for 1 min with 10000 rps against an in-memory store
    dotnet run -p Testbed -- run -d 1 -f 10000 memory
    # run for 30 mins with 2000 rps against a local EventStore
    dotnet run -p Testbed -- run -f 2000 es
    # run for two minutes against CosmosDb (see https://github.com/jet/equinox#quickstart) for provisioning instructions
    dotnet run -p Testbed -- run -d 2 cosmos

    # ... to add a Sync tool
    md -p ../My.Tools.Sync | Set-Location
    # (-m includes an example of how to upconvert from similar event-sourced representations in an existing store)
    dotnet new proSync -m
    start README.md

## CONTRIBUTING

Please don't hesitate to [create a GitHub issue](https://github.com/jet/dotnet-templates/issues/new) for any questions so others can benefit from the discussion. For any significant planned changes or additions, please err on the side of [reaching out early](https://github.com/jet/dotnet-templates/issues/new) so we can align expectations - there's nothing more frustrating than having your hard work not yielding a mutually agreeable result ;)

### Contribution guidelines - `equinox-*` templates

See [the Equinox repo's CONTRIBUTING section](https://github.com/jet/equinox/blob/master/README.md#contributing) for general guidelines wrt how contributions are considered specifically wrt Equinox.

The following sorts of things are top of the list for the `eqx*` templates at the present time:

- Fixes for typos, adding of info to the readme or comments in the emitted code etc
- Small-scale cleanup or clarifications of the emitted code
- support for additional languages in the templates
- further straightforward starter projects

While there is no rigid or defined limit to what makes sense to add, it should be borne in mind that `dotnet new eqx*` is often going to be a new user's first interaction with Equinox and/or [asp]dotnetcore. Hence there's a delicate (and intrinsically subjective) balance to be struck between:

  1. simplicity of programming techniques used / beginner friendliness
  2. brevity of the generated code
  3. encouraging good design practices