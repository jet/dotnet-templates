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

## Producer/Reactor Templates combining usage of Equinox and Propulsion

- [`proReactor`](propulsion-reactor/README.md) - Boilerplate for an application that handles reactive actions ranging from publishing notifications via Kafka (simple, or [summarising events](http://verraes.net/2019/05/patterns-for-decoupling-distsys-summary-event/) through to driving follow-on actions implied by events (e.g., updating a denormalized view of an aggregate)

   Input options are:
   
   0. (default) dual mode CosmosDB ChangeFeed Processor and/or EventStore `$all` stream projector/reactor using `Propulsion.Cosmos`/`Propulsion.EventStore` depending on whether the program is run with `cosmos` or `es` arguments
   1. `--source changeFeedOnly`: removes `EventStore` wiring from commandline processing
   2. `--source kafkaEventSpans`: changes source to be Kafka Event Spans, as emitted from `dotnet new proProjector --kafka`

   The reactive behavior template has the following options:
   
   0. Default processing shows importing (in summary form) from an aggregate in `EventStore` or a CosmosDB ChangeFeedProcessor to a Summary form in `Cosmos` 
   1. `--blank`: remove sample Ingester logic, yielding a minimal projector
   2. `--kafka` (without `--blank`): adds Optional projection to Apache Kafka using [`Propulsion.Kafka`](https://github.com/jet/propulsion) (instead of ingesting into a local `Cosmos` store). Produces versioned [Summary Event](http://verraes.net/2019/05/patterns-for-decoupling-distsys-summary-event/) feed.
   3. `--kafka --blank`: provides wiring for producing to Kafka, without summary reading logic etc
    
   Miscellaneous options:
   - `--filter` - include category filtering boilerplate

  **NOTE At present, checkpoint storage when projecting from EventStore uses Azure CosmosDB - help wanted ;)**
  
- [`proSync`](propulsion-sync/README.md) - Boilerplate for a console app that that syncs events between [`Equinox.Cosmos` and `Equinox.EventStore` stores](https://github.com/jet/equinox) using the [relevant `Propulsion`.* libraries](https://github.com/jet/propulsion), filtering/enriching/mapping Events as necessary.

## Consumer Templates combining usage of Equinox and Propulsion

- [`summaryConsumer`](propulsion-summary-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) to ingest versioned summaries produced by a `dotnet new proReactor --kafka`

- [`trackingConsumer`](propulsion-tracking-consumer/README.md) - Boilerplate for an Apache Kafka Consumer using [`Propulsion.Kafka`](https://github.com/jet/propulsion) to ingest accumulating changes in an `Equinox.Cosmos` store idempotently.

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

## TESTING

There's [no integration test for the templates yet](https://github.com/jet/dotnet-templates/issues/2), so validating a template is safe to release is unfortunately a manual process based on:

1. Generate the package (per set of changes you make locally)

    a. ensuring the template's base code compiles (see [runnable templates concept in `dotnet new` docs](https://docs.microsoft.com/en-us/dotnet/core/tools/custom-templates))

    b. packaging into a local nupkg

        $ cd ~/dotnet-templates
        $ dotnet build build.proj
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

Pssst ... the above is also what implementing [#2](https://github.com/jet/dotnet-templates/issues/2) involves!

# PATTERNS / GUIDANCE

## Use Strongly typed ids

Wherever possible, the samples strongly type identifiers, particularly ones that might naturally be represented as primitives, i.e. `string` etc.

- [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) is useful to transparently pin types in a message contract cheaply - it works well for a number of contexts:

  - Coding/decoding events using [FsCodec](https://github.com/jet/fscodec). (because Events are things that **have happened**, validating them is not a central concern as we load and fold these incontrovertible Facts)
  - Model binding in ASP.NET (because the types de-sugar to the primitives, no special support is required). _Unlike events, there are more considerations in play in this context though; often you'll want to apply validation to the inputs (representing Commands) as you map them to [Value Objects](https://martinfowler.com/bliki/ValueObject.html), [Making Illegal States Unrepresentable](https://fsharpforfunandprofit.com/posts/designing-with-types-making-illegal-states-unrepresentable/). Often, Single Case Discriminated Unions can be a better tool inb that context_

## Managing Projections and Reactions with Equinox, Propulsion and FsKafka

<a name="aggregate-module"></a>
## Aggregate module conventions

There are established conventions documented in [Equinox's `module Aggregate` overview](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#aggregate-module)

<a name="programfs"></a>
## Microservice Program.fs conventions

All the templates herein attempt to adhere to a consistent structure for the [composition root](https://blog.ploeh.dk/2011/07/28/CompositionRoot/) `module` (the one containing an Application’s `main`), consisting of the following common elements:

### `module Configuration`

_Responsible for: Loading secrets and custom configuration, supplying defaults when environment variables are not set_

Wiring up retrieval of configuration values is the most environment-dependent aspect of the wiring up of an application's interaction with its environment and/or data storage mechanisms. This is particularly relevant where there is variance between local (development time), testing and production deployments. For this reason, the retrieval of values from configuration stores or key vaults is not managed directly within the [`module Args` section](#module-args)

The `Settings` module is responsible for the following:
1. Feeding defaults into process-local Environment Variables, _where those are not already supplied_
2. Encapsulating all bindings to Configuration or Secret stores (Vaults) in order that this does not have to be complected with the argument parsing or defaulting in `module Args`

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

### `module Logging`

_Responsible for applying logging config and setting up loggers for the application_

- DO allow overriding of log level via a command line argument and/or environment variable (by passing `Args.Arguments` or values from it)

#### example

```
module Logging =

    let initialize verbose =
	Log.Logger <- LoggerConfiguration(….)
```

### `start` function

The `start` function contains the specific wireup relevant to the infrastructure requirements of the microservice - it's the sole aspect that is not expected to adhere to a standard layout as prescribed in this section.

#### example

```
let start (args : Args.Arguments) =
    …
    (yields a started application loop)
```

### `run`,  `main` functions

The `run` function formalizes the overall pattern. It is responsible for:

1. Managing the correct sequencing of the startup procedure, weaving together the above elements
2. managing the emission of startup or abnormal termination messages to the console

- DONT alter the canonical form - the processing is in this exact order for a multitude of reasons
- DONT have any application specific wire within `run` - any such logic should live within the `start` and/or `build` functions
- DONT return an `int` from `run`; let `main` define the exit codes in one place

#### example

```
let run args =
    use consumer = start args
    consumer.AwaitCompletion() |> Async.RunSynchronously
    consumer.RanToCompletion

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
        try Logging.initialize args.Verbose
            try Configuration.initialize ()
                if run args then 0 else 3
            with e -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
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
