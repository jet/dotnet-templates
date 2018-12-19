# Jet `dotnet new` Templates

This repo hosts the source for Jet's [`dotnet new`](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-new) templates. While there's presently just one, for [Equinox](https://github.com/jet/equinox), over time the intention is to add templates for other systems where relevant.

## Available Templates

- [`equinox-web`](equinox-web/readme.md) - Boilerplate for an ASP .NET Core Web App, with an associated storage-independent Backend project. At present, only F# has been implemented.

## How to use

As dictated by [the design of dotnet's templating mechanism](https://github.com/dotnet/templating/), consumption is ultimately via the .NET Core SDK's `dotnet new` CLI facility and/or associated facilities in Visual Studio, Rider etc.

In order to be able to use them, the first step is to install the asociated NuGet packages :-

1. Install a template locally: `dotnet new -i Equinox.Templates::*` - install the templates (use `dotnet new --list` to view)
2. Use dotnet new to expand the template in a given directory: `dotnet net equinox-web --help` (to see available options)

## CONTRIBUTING

Please don't hesitate to [create a GitHub issue](https://github.com/jet/dotnet-templates/issues/new) for any questions so others can benefit from the discussion. For any significant planned changes or additions, please err on the side of [reaching out early](https://github.com/jet/dotnet-templates/issues/new) so we can align expectationss - there's nothing more frustrating than having your hard work not yielding a mutually agreeable result ;)

### Contribution guidelines - `web-equinox`

See [the Equinox repo's CONTRIBUTING section](https://github.com/jet/equinox/blob/master/README.md#contributing) for general guidelines wrt how contributions are considered specifically wrt Equinox.

Examples of changes that would likely be of interest:

- Fixes for typos, adding of info to the readme or comments in the emitted code
- Small-scale cleanup or clarifications of the emitted code
- support for additional .NET languages in the templates

While there is no firm limit to what makes sense to add, it should be borne in mind that `dotnet new equinox` is often going to be a new user's first interaction with Equinox. Hence there's a delicate (and intrinsically subjective) balance to be struck between:

a) simplicity of programming techniques used / beginner friendliness
b) brevity of the generated code
c) encouraging good design practicess