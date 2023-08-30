# Jet `dotnet new` Templates [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.dotnet-templates?branchName=master)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=9?branchName=master) [![release](https://img.shields.io/github/release/jet/dotnet-templates.svg)](https://github.com/jet/dotnet-templates/releases) [![NuGet](https://img.shields.io/nuget/vpre/Equinox.Templates.svg?logo=nuget)](https://www.nuget.org/packages/Equinox.Templates) [![license](https://img.shields.io/github/license/jet/dotnet-templates.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/dotnet-templates.svg) [<img src="https://img.shields.io/badge/discord-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=discord">](https://github.com/ddd-cqrs-es/community)

This repo hosts the source for Jet's [`dotnet new`](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-new) templates.

## [Equinox](https://github.com/jet/equinox) only

These templates focus solely on Consistent Processing using Equinox Stores:

- [`eqxweb`](equinox-web/README.md) - Boilerplate for an ASP .NET Core 3 Web App, with an associated storage-independent Domain project using [Equinox](https://github.com/jet/equinox).
- [`eqxwebcs`](equinox-web-csharp/README.md) - Boilerplate for an ASP .NET Core 3 Web App, with an associated storage-independent Domain project using [Equinox](https://github.com/jet/equinox), _ported to C#_.
- [`eqxtestbed`](equinox-testbed/README.md) - Host that allows running back-to-back benchmarks when prototyping models using [Equinox]. (https://github.com/jet/equinox), using different stores and/or store configuration parameters.
- [`eqxPatterns`](equinox-patterns/README.md) - Equinox Skeleton Deciders and Tests implementing various event sourcing patterns: 
  - Managing a chain of Periods with a Rolling Balance carried forward (aka Closing the Books)
  - Feeding items into a List managed as a Series of Epochs with exactly once ingestion logic

## [Propulsion](https://github.com/jet/propulsion) related

The following templates focus specifically on the usage of `Propulsion` components:

- [`proProjector`](propulsion-projector/README.md) - Boilerplate for a Publisher application that

  * consumes events from one of:
  
    1. _(default)_ `--source cosmos`: an Azure CosmosDb ChangeFeedProcessor (typically unrolling events from `Equinox.CosmosStore` stores using `Propulsion.CosmosStore`)
 
       * `-k --parallelOnly` schedule kafka emission to operate in parallel at document (rather than accumulated span of events for a stream) level

    2. `--source eventStore`: Track an EventStoreDB >= 21.10 instance's `$all` feed using the gRPC interface (via `Propulsion.EventStoreDb`)
    
    3. `--source sqlStreamStore`: [`SqlStreamStore`](https://github.com/SQLStreamStore/SQLStreamStore)'s `$all` feed
    
    4. `--source dynamo`

  * `-k` adds publishing to Apache Kafka using [`Propulsion.Kafka`](https://github.com/jet/propulsion).
      
- [`proConsumer`](propulsion-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) (typically consuming from an app produced with `dotnet new proProjector -k`).

- [`periodicIngester`](periodic-ingester/) - Boilerplate for a service that regularly walks the content of a source, feeding it into a propulsion projector in order to manage the ingestion process using [`Propulsion.Feed.PeriodicSource`](https://github.com/jet/propulsion)

- [`proDynamoStoreCdk`](propulsion-dynamostore-cdk/README.md)

    - AWS CDK Wiring for programmatic IaC deployment of `Propulsion.DynamoStore.Indexer` and `Propulsion.DynamoStore.Notifier`

## Producer/Reactor Templates combining usage of Equinox and Propulsion

The bulk of the remaining templates have a consumer aspect, and hence involve usage of `Propulsion`.
The specific behaviors carried out in reaction to incoming events often use `Equinox components

<a name="proReactor"></a>
- [`proReactor`](propulsion-reactor/README.md) - Boilerplate for an application that handles reactive actions ranging from publishing notifications via Kafka (simple, or [summarising events](http://verraes.net/2019/05/patterns-for-decoupling-distsys-summary-event/) through to driving follow-on actions implied by events (e.g., updating a denormalized view of an aggregate)

   Input options are:
   
   0. (default) `Propulsion.Cosmos`/`Propulsion.DynamoStore`/`Propulsion.EventStoreDb`/`Propulsion.SqlStreamStore` depending on whether the program is run with `cosmos`, `dynamo`, `es`, `sss` arguments
   2. `--source kafkaEventSpans`: changes source to be Kafka Event Spans, as emitted from `dotnet new proProjector --kafka`

   The reactive behavior template has the following options:
   
   0. Default processing shows importing (in summary form) from an aggregate in `EventStore` or a CosmosDB ChangeFeedProcessor to a Summary form in `Cosmos` 
   1. `--blank`: remove sample Ingester logic, yielding a minimal projector
   2. `--kafka` (without `--blank`): adds Optional projection to Apache Kafka using [`Propulsion.Kafka`](https://github.com/jet/propulsion) (instead of ingesting into a local `Cosmos` store). Produces versioned [Summary Event](http://verraes.net/2019/05/patterns-for-decoupling-distsys-summary-event/) feed.
   3. `--kafka --blank`: provides wiring for producing to Kafka, without summary reading logic etc
    
  **NOTE At present, checkpoint storage when projecting from EventStore uses Azure CosmosDB - help wanted ;)**
  
- [`feedSource`](feed-source/) - Boilerplate for an ASP.NET Core Web Api serving a feed of items stashed in an `Equinox.CosmosStore`. See `dotnet new feedConsumer` for the associated consumption logic
- [`feedConsumer`](feed-consumer/) - Boilerplate for a service consuming a feed of items served by `dotnet new feedSource` using [`Propulsion.Feed`](https://github.com/jet/propulsion)
  
- [`summaryConsumer`](propulsion-summary-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) to ingest versioned summaries produced by a `dotnet new proReactor --kafka`.

- [`trackingConsumer`](propulsion-tracking-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) to ingest accumulating changes in an `Equinox.Cosmos` store idempotently.

- [`proSync`](propulsion-sync/README.md) - Boilerplate for a console app that that syncs events between [`Equinox.Cosmos` and `Equinox.EventStore` stores](https://github.com/jet/equinox) using the [relevant `Propulsion`.* libraries](https://github.com/jet/propulsion), filtering/enriching/mapping Events as necessary.

- [`proArchiver`](propulsion-archiver/README.md) - Boilerplate for a console app that that syncs Events from relevant Categories from a Hot container and to an associated warm [`Equinox.Cosmos` stores](https://github.com/jet/equinox) archival container using the [relevant `Propulsion`.* libraries](https://github.com/jet/propulsion).
    - An Archiver is intended to run continually as an integral part of a production system.

- [`proPruner`](propulsion-pruner/README.md) - Boilerplate for a console app that that inspects Events from relevant Categories in an [`Equinox.Cosmos` store's](https://github.com/jet/equinox) Hot container and uses that to drive the removal of (archived) Events that have Expired from the associated Hot Container using the [relevant `Propulsion`.* libraries](https://github.com/jet/propulsion).
    
    - While a Pruner does not consume a large amount of RU capacity from either the Hot or Warm Containers, running one continually is definitely optional; a Pruner only has a purpose when there are Expired events in the Hot Container; running periodically during low-load periods may be appropriate, depending on the lifetime profile of the events in your system
    
    - Reducing the traversal frequency needs to be balanced against the primary goal of deleting from the Hot Container: preventing it splitting into multiple physical Ranges.
    
    - It is necessary to reset the CFP checkpoint (delete the checkpoint documents, or use a new Consumer Group Name) to trigger a re-traversal if events have expired since the lsat time a traversal took place.

- [`proIndexer`](propulsion-cosmos-reactor/README.md) - Derivative of `proReactor` template. :pray: [@ragiano215](https://github.com/ragiano215)

    - Specific to CosmosDB, though it would be easy to make it support DynamoDB

    - For applications where the reactions using the same Container, credentials etc as the one being Monitored by the change feed processor (simpler config wiring and less argument processing)

    - includes full wiring for Prometheus metrics emission from the Handler outcomes

    - Demonstrates notion of an `App` project that hosts common wiring common to a set of applications without having the Domain layer reference any of it. 
  
    - Implements `sync` and `snapshot` subcommands to enable updating snapshots and/or keeping a cloned database in sync

<a name="eqxShipping"></a>
- [`eqxShipping`](equinox-shipping/README.md) - Example demonstrating the implementation of a [Process Manager](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html) using [`Equinox`](https://github.com/jet/equinox) that manages the enlistment of a set of `Shipment` Aggregate items into a separated `Container` Aggregate as an atomic operation. :pray: [@Kimserey](https://github.com/Kimserey).
 
   - processing is fully idempotent; retries, concurrent or overlapping transactions are intended to be handled thoroughly and correctly
   - if any `Shipment`s cannot be `Reserved`, those that have been get `Revoked`, and the failure is reported to the caller
   - includes a `Watchdog` console app (based on `dotnet new proReactor --blank`) responsible for concluding abandoned transaction instances (e.g., where processing is carried out in response to a HTTP request and the Clients fails to retry after a transient failure leaves processing in a non-terminal state).
   - Does not include wiring for Prometheus metrics (see `proHotel`)

<a name="proHotel"></a>
- [`proHotel`](propulsion-hotel/README.md) - Example demonstrating the implementation of a [Process Manager](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html) using [`Equinox`](https://github.com/jet/equinox) that coordinates the merging of a set of `GuestStay`s in a Hotel as a single `GroupCheckout` activity that coves the payment for each of the stays selected.

    - illustrates correct idempotent logic such that concurrent group checkouts that are competing to cover the same stay work correctly, even when commands are retried.
    - Reactor program is wired to support consuming from `MessageDb` or `DynamoDb`.
    - Unit tests validate correct processing of reactions without the use of projection support mechanisms from the Propulsion library.
    - Integration tests establish a Reactor an xUnit.net Collection Fixture (for MessageDb or DynamoDb) or Class Fixtures (for MemoryStore) to enable running scenarios that are reliant on processing that's managed by the Reactor program, without having to run that concurrently.
    - Includes wiring for Prometheus metrics.

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

    # ... to add an Ingester that reacts to events, as they are written (via EventStore $all or CosmosDB ChangeFeedProcessor) summarising them and feeding them into a secondary stream
    # (equivalent to pairing the Projector and Ingester programs we make below)
    md -p ../DirectIngester | Set-Location
    dotnet new proReactor
    
    # ... to add a Projector
    md -p ../Projector | Set-Location
    # (-k emits to Kafka and hence implies having a Consumer)
    dotnet new proProjector -k
    start README.md

    # ... to add a Generic Consumer (proProjector -k emits to Kafka and hence implies having a Consumer)
    md -p ../Consumer | Set-Location
    dotnet new proConsumer
    start README.md

    # ... to add an Ingester based on the events that Projector sends to kafka
    # (equivalent in function to DirectIngester, above)
    md -p ../Ingester | Set-Location
    dotnet new proReactor --source kafkaEventSpans

    # ... to add a Summary Projector
    md -p ../SummaryProducer | Set-Location
    dotnet new proReactor --kafka 
    start README.md

    # ... to add a Custom Projector
    md -p ../SummaryProducer | Set-Location
    dotnet new proReactor --kafka --blank
    start README.md

    # ... to add a Summary Consumer (ingesting output from `SummaryProducer`)
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

    # ... to add a Shipping Domain example containing a Process Manager with a Watchdog Service
    md -p ../Shipping | Set-Location
    dotnet new eqxShipping

    # ... to add a Reactor against a Cosmos container for both listening and writing
    md -p ../Indexer | Set-Location
    dotnet new proIndexer

    # ... to add a Hotel Sample for use with MessageDb or DynamoDb
    md -p ../ProHotel | Set-Location
    dotnet new proHotel

## TESTING

There's [integration tests in the repo](https://github.com/jet/dotnet-templates/blob/int-tests/tests/Equinox.Templates.Tests/DotnetBuild.fs) that check everything compiles before we merge/release

    dotnet build build.proj # build Equinox.Templates package, run tests \/
    dotnet pack build.proj # build Equinox.Templates package only
    dotnet test build.proj -c Release # Test aphabetically newest file in bin/nupkgs only (-c Release to run full tests)

One can also do it manually:

1. Generate the package (per set of changes you make locally)

    a. ensuring the template's base code compiles (see [runnable templates concept in `dotnet new` docs](https://docs.microsoft.com/en-us/dotnet/core/tools/custom-templates))

    b. packaging into a local nupkg

        $ cd ~/dotnet-templates
        $ dotnet pack build.proj
        Successfully created package '/Users/me/dotnet-templates/bin/nupkg/Equinox.Templates.3.10.1-alpha.0.1.nupkg'.

2. Test, per variant

    (Best to do this in another command prompt in a scratch area)

    a. installing the templates into the `dotnet new` local repo

        $ dotnet new -i /Users/me/dotnet-templates/bin/nupkg/Equinox.Templates.3.10.1-alpha.0.1.nupkg

    b. get to an empty scratch area

        $ mkdir -p ~/scratch/templs/t1
        $ cd ~/scratch/templs/t1
    
    c. test a variant (i.e. per `symbol` in the config)

        $ dotnet new proReactor -k # an example - in general you only need to test stuff you're actually changing
        $ dotnet build # test it compiles
        $ # REPEAT N TIMES FOR COMBINATIONS OF SYMBOLS

3. uninstalling the locally built templates from step 2a:

      $ dotnet new -u Equinox.Templates

<a name="guidance"></a>
# PATTERNS / GUIDANCE

<a name="tldr"></a>
## TL;DR

1. ✅ DO have global strongly typed id types in `namespace Domain`
2. ❌ DONT have global `module Types`. AVOID per Aggregate `module Types` or top level `type` definitions
3. ✅ DO group stuff predictably per `module Aggregate`: `Stream, Events, Reactions, Fold, Decide, Service, Factory`. And keep grouping within that.
4. ❌ DONT [`open <Aggregate>`](#dont-open-aggregate), [`open <Aggregate>.Events`](#dont-open-events) or [`open <Aggregate>.Fold`](#dont-open-fold)
5. ✅ DO design for idempotency everywhere. ❌ DONT [return TMI](#dont-return-tmi) that the world should not be taking a dependency on. 
6. ❌ DONT [use `Result`](#dont-result) or a per-Aggregate `type Error`. ✅ [DO use minimal result types per decision function](#do-simplest-result)
7. ❌ DONT [expose your `Fold.State`](#dont-expose-state) outside your Aggregate.
8. ❌ DONT be a slave to CQRS for all read paths. ✅[DO `AllowStale`](#do-allowstale). [CONSIDER `QueryCurrent`](#consider-querycurrent).
9. ❌ [DONT be a slave to the Command pattern](#dont-commands)

## High level

### ❌ DONT have a global `Types.fs`

F# really shines at succinctly laying out a high level design for a system; see [_Designing with types_ by Scott Wlaschin for many examples](https://fsharpforfunandprofit.com/series/designing-with-types/).

For an event sourced system, if anything, this is even more true - it's not uncommon to be able to convey the key moving parts of a system in a manner that's legible for both technical and non-technical stakeholders.

It's important not to take this too far though - ultimately as a system grows, the key constraint of the fact that Events ultimately need to be Grouped in Categories of Streams needs to become then organising function.

<a name="global-dont-share-types"></a>
### ❌ DONT share types across Aggregates / Categories

There are sometimes legitimate when cases where two Aggregates have overlapping concerns. It can be very tempting to put the common types into a central place and Just Share the contracts. This should be avoided. Instead:

```fs
❌ DONT DO THIS

// <Types.fs>
module Domain.Types

type EntityContext = { name: string; area: string }

...

// <Aggregate>.fs
module Aggregate

open Domain.Types

module Events =

    type Event =
        | Created of {| creator: UserId; context: EntityContext |}
        ...

// <Aggregate2>.fs
module Aggregate2

module Events =

    type Event =
        | Copied of {| by: UserId; context: Types.EntityContext |}
        ...
```

Instead:
- have each `module <Aggregate>` have its own version of each type that will be used in an event _within its `module Events`_. (The `decide` function can map from an input type if desired, but the important thing is that the Aggregate will need to be able to roundtrip its types in perpetuity, and having to disentangle the overlap between more than on Aggregate is simply never a good tradeoff) 
- [sharing id types is fine](#global-do-share-ids)

<a name="global-do-share-ids"></a>
### ✅ DO have global strongly typed ids

While [sharing the actual types is a no-no](#global-dont-share-types), having common id types is perfectly reasonable

```fsharp
module Domain.Types

type UserId = ...
type TenantId = ...

...

module Domain.User

module Events =

    type Joined = { tenant: TenantId; authorizedBy: UserId }

```

- CONSIDER UMX for non-serialized ids

Wherever possible, the templates use use strongly type identifiers, particularly ones that might naturally be represented as primitives, i.e. `string` etc.

- [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) is useful to transparently pin types in a message contract cheaply - it works well for a number of contexts:

    - Coding/decoding events using [FsCodec](https://github.com/jet/fscodec). (because Events are things that **have happened**, validating them is not a central concern as we load and fold these incontrovertible Facts)
    - Model binding in ASP.NET (because the types de-sugar to the primitives, no special support is required). _Unlike events, there are more considerations in play in this context though; often you'll want to apply validation to the inputs (representing Commands) as you map them to [Value Objects](https://martinfowler.com/bliki/ValueObject.html), [Making Illegal States Unrepresentable](https://fsharpforfunandprofit.com/posts/designing-with-types-making-illegal-states-unrepresentable/). Often, Single Case Discriminated Unions can be a better tool inb that context_

### ✅ DO Have a helper `module` per id type

TODO write this up

### CONSIDER UMX `Guid`s for serialized ids

TODO write this up

### CONSIDER UMX `strings` for serialized ids

TODO write this up

## Code structure

### 1. `module Aggregate`

<a name="aggregate-module"></a>
#### ✅ DO stick to the `module <Aggregate>` conventions

There are established conventions documented in [Equinox's `module Aggregate` overview](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#aggregate-module)

#### ❌ DONT split the `module <Aggregate>`
Having the Event Contracts, State and Decision logic in a single module can feel wrong when you get over e.g. 1000 lines of code; instincts to split the file on some basis will kick in. Don't do it; splitting the file is hiding complexity under the carpet.

#### ❌ DONT move the `module Events` out

The Event Contracts are the most important contract that an Aggregate has - decision logic will churn endlessly. You might even implement logic against it in other languages. But the Event Contracts you define are permanent. As a developer fresh to a project, the event contracts are often the best starting point as you try to understand what a given aggregate is responsible for. 

#### ❌ DONT move the `module State`, or `evolve` logic out

The State type and the associated `evolve` and `fold` functions are intimately tied to the Event Contracts. Over time, ugliness and upconversion can lead to noise, and temptation to move it out. Don't do it; being able to understand the full coupling is critical to understanding how things work, and equally critical to being able to change or add functions.

#### ❌ DONT move the decision logic out

Decision logic bridges between the two worlds of State and Events. The State being held exists only to serve the Decision logic. The only reason for Event Contracts is to record Decisions. Trying to pretend that some of the Decisions are less important and hence should live elsewhere is rarely a good idea. How decisions are made, and how those decisions are encoded as Events should be encapsulated within the Aggregate.

### 2. `module Events`

Having the Event Contracts be their own `module` is a critical forcing function for good aggregate design. Having all types and all cases live in one place and being able to quickly determine where each Event is produced is key to being able to understand the moving parts of a system.

<a name="events-no-ids"></a>
#### ❌ AVOID including egregious identity information

When modelling, it's common to include primary identifiers (e.g. a user id), or contextual identifiers (e.g. a tenant id) in an Event in order to convey the relationships between events in the systems as a whole; you want the correlations to stand out. In the implementation however, repeating the identity information in every event is a major liability:
1. the State needs to contain the values - that's more noise
2. Event versioning gets messier - imagine extending a system to make it multi-tenant, you'd need to be able to handle all the historic events that predated the concept

The alternative is for a workflow to react to the events in the context of a stream - if some logic needs to know the userid let the User reactor handing the User event on a User Stream pass thgat context forward if relevant in that context.

#### ❌ DONT `open Events` in an aggregate module

Having to prefix types and/or Event Type names with `Events.` is a feature, not a bug. 

### 3. `module Fold`

<a name="fold-no-log"></a>
#### ❌ DONT log

If Fold logic is not incredibly boring, that's a design smell. In general, you want to [make illegal States unrepresentable](https://fsharpforfunandprofit.com/posts/designing-with-types-making-illegal-states-unrepresentable/). If you must, unit test it to satisfy yourself things can't go wrong.

#### ❌ DONT maintain identifiers and other information not required for decisions

See [Events: AVOID including egregious identity information](#events-no-ids).

### 4. `module Decisions`

<a name="do-simplest-result"></a>
#### ✅ DO use the simplest result type possible

[Railway Oriented programming](https://fsharpforfunandprofit.com/rop) is a fantastic thinking tool. [Designing with types](https://fsharpforfunandprofit.com/series/designing-with-types/) is an excellent implementation strategy. [_Domain Modelling Made Functional_](https://fsharpforfunandprofit.com/books/) is a must read book. But it's critical to also consider the other side of the coin to avoid a lot of mess:
- [_Against Railway Oriented Programming_ by Scott Wlaschin](https://fsharpforfunandprofit.com/posts/against-railway-oriented-programming/). Scott absolutely understands the tradeoffs, but it's easy to forget them when reading the series 
- [_you're better off using Exceptions_ by Eirik Tsarpalis](https://eiriktsarpalis.wordpress.com/2017/02/19/youre-better-off-using-exceptions).

Each Decision function should have as specific a result contract as possible. In order of preference:
- `unit`: A function that idempotently maps the intent or request to internal Events based solely on the State is the ideal. Telling the world about what you did is not better. Logging what it did is not better than being able to trust it to do it's job. Unit tests should assert based on the produced Events as much as possible rather than relying on a return value.
- `throw`: if something can go wrong, but it's not an anticipated first class part of the workflow, there's no point returning an `Error` result; [_you're better off using Exceptions_](https://eiriktsarpalis.wordpress.com/2017/02/19/youre-better-off-using-exceptions).
- `bool`: in some cases, an external system may need to know whether something is permitted or necessary. If that's all that's needed, don't return identifiers or messages give away extra information
- _simple discriminated union_: the next step after a `true`/`false` is to make a simple discriminated union - you get a chance to name it, and the cases involved.
- record, anonymous record, tuple: returning multiple items is normally best accomplished via a named record type.
  - the caller gets to use a clear name per field
  - how it's encoded in the State type can vary over time without consumption sites needing to be revisited
  - extra fields can be added later, without each layer through which the response travels needing to be adjusted
  - the caller gets to pin the exact result type via a type annotation (either in the `Service`'s `member` return type, or at the call site) - this is not possible if it's an anonymous record
  :bulb: in some cases it a tuple can be a better encoding if it's important that each call site explicitly consume each part of the result
- `string`: A string can be anything in any language. It can be `null`. It should not be used to convey a decision outcome.
- `Result`: A result can be a success or a failure. both sides are generic. Its the very definition of a lowest common denominator.
  - if it's required in a response transmission, map it out there; don't make the implementation logic messier and harder to test in order to facilitate that need.
  - if it's because you want to convey some extra information that the event cannot convey, use a tuple, a record or a Discriminated Union 

#### ❌ DONT Log

It's always sufficient to return a `bool` or `enum` to convey an outcome (but try to avoid even that). See [Fold: DONT log](#fold-no-log)

<a name="dont-result"></a>
#### ❌ DONT use a `Result` type

Combining success and failures into one type because something will need to know suggests that there is a workflow. It's better to model that explicitly.

If your API has a common set of result codes that it can return, map to those later - the job here is to model the decisions.

See [use the simplest result possible](#decide-results-simple).

<a name="dont-return-tmi"></a>
#### ❌ DONT return more status than necessary

A corollary of designing for idempotency is that we don't want to have the caller care about whether a request triggered a change. If we need to test that, we can call the decision function and simply assert against the events it produced.

```fsharp
// ❌ DONT DO THIS!
module Decisions =

    let create name state =
        if state <> Initial then AlreadyCreated, [||]
        else Ok, [| Created { name = name } |] 
```
The world does not need to know that you correctly handled at least once delivery of a request that was retried when the wifi reconnected.

Instead:
```fsharp
let create name = function
    | Fold.Initial -> [| Events.Created { name = name } |]
    | Fold.Running _ -> [||]

...

module ThingTests

let [<Fact>] ``create generates Created`` () =
    let state = Fold.Initial
    let events = Decisions.create "tim" state
    events =! [| Events.Created { name = "tim" } |]
    
let [<Fact>] ``create is idempotent`` () =
    let state = Fold.Running ()
    let events = Decisions.create "tim" state
    events =! [||]
```


#### ❌ DONT share a common result type across multiple decision functions

If you have three outcomes for one decision, don't borrow that result type for a separate decision that only needs two. Just give it it's own type. See [use the simplest result possible](#decide-results-simple).

#### ✅ DO partition decision logic

Most systems will have a significant number of Aggregates with low numbers of Events and Decisions. Having the Decision functions at the top level of the Aggregate Module can work well for those. Many people like to group such logic within a `module Decisions`, as it gives a good outline (`module Stream`, `module Events`, `module Reactions`, `module Fold`, `type Service`, `module Factory`) that allows one to quickly locate relevant artifacts and orient oneself in a less familiar area of the code. A key part of managing the complexity is to start looking for ways to group them into clumps of 3-10 related decision functions in a `module` within the overall `module Decisions` (or at top level in the file) as early as possible.

<a name="dont-commands"></a>
#### ❌ DONT be a slave to the Command pattern

The bulk of introductory material on the Decider pattern, and event sourcing in general uses the Command pattern as if it's a central part of the architecture. That's not unreasonable; it's a proven pattern that's useful in a variety of contexts.

Some positives of the pattern are:
- one can route any number of commands through any number of layers without having to change anything to add a new command
- it can be enable applying cross-cutting logic uniformly
- when implemented as Discriminated Unions in F#, the code can be very terse, and you can lean on total matching etc.
- In some cases it can work well with property based testing; the entirety of an Aggregate's Command Handling can be covered via Property Based Testing etc

However, it's also just a pattern. It has negatives; some:
- if you have a single command handler, the result type is forced to be a lowest common denominator
- the code can actually end up longer and harder to read, but still anaemic in terms of modelling the domain properly

    ```fsharp
    module Decisions =
        type Command = Increment | Decrement
        let decide command state =
            match command with
            | Increment by -> if state = 10 then [||] else [| Events.Incremented |]
            | Decrement -> if state = 0 then [|] else [| Events.Decremented |]
            | Reset -> if state = 0 then [||] else [| Events.Reset |]
    type Service(resolve: ...) =
        member _.Execute(id, c) =
            let decider = resolve id
            decider.Transact(Decisions.decide c)
    type App(service: Service, otherService: ...) =
        member _.Execute(id, cmd) =
            if otherService.Handle(id, cmd) then 
                service.Execute(id, cmd)
    type Controller(app: App) =
        member _.Reset(id) =
            app.Execute(id, Aggregate.Command.Reset)
    ```

    If you instead use methods with argument lists to convey the same information, there's more opportunity to let the intention be conveyed in the code.

    ```fsharp
    module Decisions =
        let increment state = [| if state < 10 then Events.Incremented |]
        let reset _state = [| if state <> 0 then Events.Reset |]
    type Service(resolve: ...) =
        member _.Reset id =
            let decider = resolve id
            decider.Transact Decisions.reset
        member _.Increment(id, ?by) =
            let decider = resolve id
            decider.Transact Decisions.increment
    type App(service: Service, otherService: ...) =
        member _.HandleFrob(id) =
            if otherService.AttemptFrob() then
                service.Increment(id)
        member _.Reset(id) =
            service.Reset(id)
    type Controller(app: App) =
        member _.Frob() =
            app.HandleFrob id
        member _.Reset() =
            app.Reset id
    ```

<a name="module-queries"></a>
### 5. `module Queries`

The primary purpose of an Aggregate is to gather State and produce Events to facilitate making and recording of Decisions. There is no Law Of Event Sourcing that says you must at all times use CQRS to split all reads out to some secondary derived read model.

In fact, in the the context of Equinox, the `AccessStrategy.RollingState`, `LoadOption.AllowStale` and `LoadOption.AnyCachedState` features each encourage borrowing the Decision State to facilitate rendering that state to users of the system directly.

However, making pragmatic choices can also become unfettered hacking very quickly. As such the following apply.

#### ✅ DO use a `module Queries`

Unless there is a single obvious boring rendition for a boring aggregate, you should have a type per Queyr

#### ✅ DO use view DTOs

As with the guidance on [not using Lowest Common Denominator representations for results](#decide-results-simple), you want to avoid directly exposing the State

<a name="dont-expose-state"></a>
##### ❌ DONT having a public generic `Read` function that exposes the `Fold.State`

The purpose of the Fold State is to facilitate making decisions correctly. It often has other concerns such as:
- being able to store and reload from a snapshot
- being able to validate inferences being made based on events are being made correctly in the context of tests
    
Having it also be a read model DTO is a bridge too far:

```fs
// ❌ DONT DO THIS!
member service.Read(tenantId) =
    let decider = resolve tenantId
    decider.Query(fun state -> state)
```

<a name="do-allowstale"></a>
#### CONSIDER `ReadCached*` methods delegating to an internal generic `Query` with a `maxAge`:

`LoadOption.AllowStale` is the preferred default strategy for all queries. This is for two reasons:
1. if a cached version of the state fresher than the `maxAge` tolerance is available, you produce a result immediately and your store does less work
2. even if a sufficiently fresh state is not available, all such reads are coalesced into a single store roundtrip. This means that the impact of read traffic on the workload hitting the store itself is limited to one read round trip per `maxAge` interval. 

```fsharp
module Queries =

    let infoCachingPeriod = TimeSpan.FromSeconds 10.
    type NameInfo = { name: string; contact: ContactInfo }
    let renderName (state: Fold.State) = { name = state.originalName; contact = state.contactDetails } 
    let renderPendingApprovals (state: Fold.State) = Fold.calculatePendingApprovals state

type Service(resolve: ...)

    // NOTE: Query should remain private; expose each relevant projection as a `Read*` method
    member private service.Query(maxAge: TimeSpan, tenantId, render: Fold.State -> 'r): Async<'r> =
        let decider = resolve tenantId
        decider.Query(render, load = Equinox.LoadOption.AllowStale maxAge)
  
    member service.ReadCachedName(tenantId): Async<Queries.NameInfo> =
        service.Query(Queries.infoCachingPeriod, Queries.renderName)      
    member service.ReadPending(tenantId): Async<int> =
        service.Query(Queries.infoCachingPeriod, Queries.renderPendingApprovals)      
```

<a name="consider-querycurrent"></a>
#### CONSIDER `QueryCurrent*` methods delegating to a `QueryRaw` helper

While the `ReadCached*` pattern above is preferred, as it protect the store from unconstrained read traffic, there are cases where it's deemed necessary to be able to [Read Your Writes](https://www.allthingsdistributed.com/2007/12/eventually_consistent.html) 'as much as possible' at all costs.

_TL;DR quite often you should really be doing the [`ReadCached` pattern](#do-allowstale)_

The first thing to note is that you need to be sure you're actually meeting that requirement. For instance, if you are using EventStoreDB, DynamoDB or MessageDB, you will want to use `Equinox.LoadOption.RequireLeader` for it to be meaningful (otherwise a read, (yes, even one served from the same application instance) might be read from a replica that has yet to see the latest state). For [CosmosDB in `Session` consistency mode, similar concerns apply](https://github.com/jet/equinox/issues/192).

It's also important to consider the fact that any read, no matter how consistent it is at the point of reading, is also instantly stale data the instant it's been performed.

:warning: If each and every query that is processed results in a store roundtrip, and you don't have any natural limiting of the request traffic, you open yourself up to overloading the store with read traffic (which is a primary reason the CQRS pattern is considered a good default). [`AllowStale` mode](#do-allowstale) is less prone to this issue, as store read round trips are limited to one per `maxAge` interval.

:warning: `QueryRaw` should stay `private` - you want to avoid having read logic spread across your application doing arbitrary reads that are not appropriately encapsulated within the Aggregate.
  
```fs
// NOTE: the QueryRaw helper absolutely needs to stay private. Expose queries only as specific `QueryCurrent*` methods  
member private service.QueryRaw(tenantId, render) =
    let decider = resolve tenantId
    decider.Query(render, Equinox.LoadOption.RequireLeader)

member service.QueryCurrentState(tenantId) =
    service.QueryRaw(Queries.renderState)        
```

## Outside `module <Aggregate>`

<a name="dont-open-aggregate"></a>
### ❌ DONT `open <Aggregate>`

Ideally use the full name. If you can't help it, [use `module` aliases as outlined below](#dont-open-events) instead. If you are opening it because you also need to touch the Fold State, [don't do that either](#dont-open-fold).

Exception: for the unit tests associated with a single Aggregate, `open Aggregate` may make sense. As long as it's exactly that one `Aggregate`

<a name="dont-open-events"></a>
### ❌ DONT `open <Aggregate>.Events`

If you have logic in another module that is coupled to an event contract, you want that to stick out.
1. If the module is concerned with exactly one Aggregate, you can alias it via: `module Events = Aggregate.Events`
2. If the module is concerned with more than one Aggregate and there are less than 10 usages, prefix the consumption with `Aggregate.Events.`
3. If the module is concerned with more than one Aggregate and there are many usages, or the name is long, alias it via `module AggEvents = AggregateWithLongName.Events.`

Exception: In some cases, an `open Events`, _inside_ `module Fold` might be reasonable:

```fsharp
module Events = 

    ...
    
 module Fold = 
     open Events
     let evolve state = function
         | Increment -> state + 1
         | Decrement -> state - 1
```

BUT, how much worse is it to have to read or type:

```fsharp
module Events = 

    ...
    
module Fold = 

    let evolve state = function
        | Events.Increment -> state + 1
        | Events.Decrement -> state - 1
```

Within `module Decisions`, it's normally best not to open it. i.e. whenever producing Events, simply prefix it:

```fsharp
module Events = 

    ...
    
 module Decisions = 

     module Counting =
     
         let increment state = [| if state < 10 then Events.Incremented |]
```

<a name="dont-open-fold"></a>
### ❌ DONT `open <Aggregate>.Fold`

If you have external logic that is coupled to the State of an Aggregate and/or the related types, be explicit about that coupling; refer to `Aggregate.Fold.State` to make it clear. Or use the `ReadCached*` or `QueryCurrent*` patterns, which by definition return a specific type that is not the full `State` (and is not in the `Fold` namespace/module).

# Managing Projections and Reactions with Equinox, Propulsion and FsKafka

<a name="programfs"></a>
## Microservice Program.fs conventions

All the templates herein attempt to adhere to a consistent structure for the [composition root](https://blog.ploeh.dk/2011/07/28/CompositionRoot/) `module` (the one containing an Application’s `main`), consisting of the following common elements:

### `type Configuration`

_Responsible for: Loading secrets and custom configuration, supplying defaults when environment variables are not set_

Wiring up retrieval of configuration values is the most environment-dependent aspect of the wiring up of an application's interaction with its environment and/or data storage mechanisms. This is particularly relevant where there is variance between local (development time), testing and production deployments. For this reason, the retrieval of values from configuration stores or key vaults is not managed directly within the [`module Args` section](#module-args)

The `Configuration` type is responsible for encapsulating all bindings to Configuration or Secret stores (Vaults) in order that this does not have to be complected with the argument parsing or defaulting in `module Args`

- DO (sparingly) rely on inputs from the command line to drive the lookup process
- DONT log values (`module Args`’s `Arguments` wrappers should do that as applicable as part of the wireup process)
- DONT perform redundant work to load values if they’ve already been supplied via Environment Variables

### `module Args`

_Responsible for: mapping Environment Variables and the Command Line `argv` to an `Arguments` model_

`module Args` fulfils three roles:

1. uses [Argu](http://fsprojects.github.io/Argu/tutorial.html) to map the inputs passed via `argv` to values per argument, providing good error and/or help messages in the case of invalid inputs
2. responsible for managing all defaulting of input values _including echoing them to console such that an operator can infer the arguments in force_ without having to go look up defaults in a source control repo
3. expose an object model that the `build` or `start` functions can use to succinctly wire up the dependencies without needing to touch `Argu`, `Configuration`, or any concrete Configuration or Secrets storage mechanisms

- DO take values via Argu or Environment Variables
- DO log the values being applied, especially where defaulting is in play
- DONT log secrets
- DONT mix in any application or settings specific logic (**no retrieval of values, don’t make people read the boilerplate to see if this app has custom secrets retrieval**)
- DONT invest time changing the layout; leaving it consistent makes it easier for others to scan
- DONT be tempted to merge blocks of variables into a coupled monster - the intention is to (to the maximum extent possible) group arguments into clusters of 5-7 related items
- DONT reorder types - it'll just make it harder if you ever want to remix and/or compare and contrast across a set of programs

NOTE: there's a [medium term plan to submit a PR to Argu](https://github.com/fsprojects/Argu/issues/143) extending it to be able to fall back to environment variables where a value is not supplied, by means of declarative attributes on the Argument specification in the DU, _including having the `--help` message automatically include a reference to the name of the environment variable that one can supply the value through_

### `type Logging`

_Responsible for applying logging config and setting up loggers for the application_

- DO allow overriding of log level via a command line argument and/or environment variable (by passing `Args.Arguments` or values from it)

#### example

```
type Logging() =

    [<Extension>]
    static member Configure(configuration : LoggingConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        // etc.
```

### `start` function

The `start` function contains the specific wireup relevant to the infrastructure requirements of the microservice - it's the sole aspect that is not expected to adhere to a standard layout as prescribed in this section.

#### example

```
let start (args : Args.Arguments) =
    …
    (yields a started application loop)
```

### `run`,  `main` function

The `run` function formalizes the overall pattern. It is responsible for:

1. Managing the correct sequencing of the startup procedure, weaving together the above elements
2. managing the emission of startup or abnormal termination messages to the console

- DONT alter the canonical form - the processing is in this exact order for a multitude of reasons
- DONT have any application specific wire within `run` - any such logic should live within the `start` and/or `build` functions
- DONT return an `int` from `run`; let `main` define the exit codes in one place

#### example

```
let run args = async {
    use consumer = start args
    return! consumer.AwaitWithStopOnCancellation()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
```

## CONTRIBUTING

Please don't hesitate to [create a GitHub issue](https://github.com/jet/dotnet-templates/issues/new) for any questions, so others can benefit from the discussion. For any significant planned changes or additions, please err on the side of [reaching out early](https://github.com/jet/dotnet-templates/issues/new) so we can align expectations - there's nothing more frustrating than having your hard work not yielding a mutually agreeable result ;)

See [the Equinox repo's CONTRIBUTING section](https://github.com/jet/equinox/blob/master/README.md#contributing) for general guidelines wrt how contributions are considered specifically wrt Equinox.

The following sorts of things are top of the list for the templates:

- Fixes for typos, adding of info to the readme or comments in the emitted code etc
- Small-scale cleanup or clarifications of the emitted code
- support for additional languages in the templates
- further straightforward starter projects

While there is no rigid or defined limit to what makes sense to add, it should be borne in mind that `dotnet new eqx/pro*` is sometimes going to be a new user's first interaction with Equinox and/or [asp]dotnetcore. Hence there's a delicate (and intrinsically subjective) balance to be struck between:

  1. simplicity of programming techniques used / beginner friendliness
  2. brevity of the generated code
  3. encouraging good design practices

  In other words, there's lots of subtlety to what should and shouldn't go into a template - so discussing changes before investing time is encouraged; agreed changes will generally be rolled out across the repo.
