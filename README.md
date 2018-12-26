# Jet `dotnet new` Templates

This repo hosts the source for Jet's [`dotnet new`](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-new) templates. While there's presently just one, for [Equinox](https://github.com/jet/equinox), over time the intention is to add templates for other systems where relevant.

## Available Templates

- [`equinox-web`](equinox-web/readme.md) - Boilerplate for an ASP .NET Core Web App, with an associated storage-independent Backend project. At present, only F# has been implemented.

## How to use

As dictated by [the design of dotnet's templating mechanism](https://github.com/dotnet/templating/), consumption is ultimately via the .NET Core SDK's `dotnet new` CLI facility and/or associated facilities in Visual Studio, Rider etc.

To use from the command line, the outline is:
  1. Install a template locally (use `dotnet new --list` to view your current list)
  2. Use `dotnet new` to expand the template in a given directory

    dotnet new -i Equinox.Templates
    dotnet new equinoxweb -t --help
    dotnet run -p Web

   3. Run requests against that endpoint, e.g.:

    start https://www.todobackend.com/client/index.html?https://localhost:5001/todos

    _NB Jet does now own, control or audit https://todobackend.com; it is a third party site; please satisfy yourself that this is a safe thing use in your nevironment before using it._

## CONTRIBUTING

Please don't hesitate to [create a GitHub issue](https://github.com/jet/dotnet-templates/issues/new) for any questions so others can benefit from the discussion. For any significant planned changes or additions, please err on the side of [reaching out early](https://github.com/jet/dotnet-templates/issues/new) so we can align expectationss - there's nothing more frustrating than having your hard work not yielding a mutually agreeable result ;)

### Contribution guidelines - `equinox-web`

See [the Equinox repo's CONTRIBUTING section](https://github.com/jet/equinox/blob/master/README.md#contributing) for general guidelines wrt how contributions are considered specifically wrt Equinox.

The following sorts of things are top of the list for the `equinox-web` template at the present time:

- Fixes for typos, adding of info to the readme or comments in the emitted code etc
- Small-scale cleanup or clarifications of the emitted code
- support for additional .NET languages in the templates
- further straightforward starter projects

While there is no rigid or defined limit to what makes sense to add, it should be borne in mind that `dotnet new equinoxweb` is often going to be a new user's first interaction with Equinox. Hence there's a delicate (and intrinsically subjective) balance to be struck between:

  1. simplicity of programming techniques used / beginner friendliness
  2. brevity of the generated code
  3. encouraging good design practices