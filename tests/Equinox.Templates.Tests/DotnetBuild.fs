namespace Equinox.Templates.Tests

open Xunit
open Xunit.Abstractions

type TemplatesToTest() as this =
    inherit TheoryData<string, string list>()
    do for t in ["eqxweb"; "eqxwebcs"] do
        this.Add(t, ["--todos"])
        this.Add(t, ["--todos"; "--memoryStore"])
        this.Add(t, ["--todos"; "--eventStore"])
        this.Add(t, ["--todos"; "--cosmos"])
    do this.Add("eqxTestbed", [])
    do this.Add("proProjector", [])
    do this.Add("proProjector", ["--kafka"])
    do this.Add("proProjector", ["--kafka --parallelOnly"])

type DotnetBuild(output : ITestOutputHelper, folder : EquinoxTemplatesFixture) =

    let [<Theory; ClassData(typeof<TemplatesToTest>)>] ``dotnet runs`` (template, args) =
        output.WriteLine(sprintf "using %s" folder.PackagePath)
        let folder = Dir.cleared template
        Dotnet.instantiate folder template args
        Dotnet.build [folder]

    interface IClassFixture<EquinoxTemplatesFixture>

module Dummy = let [<EntryPoint>] main argv = 0