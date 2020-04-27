namespace Equinox.Templates.Tests

open Xunit
open Xunit.Abstractions

type TemplatesToTest() as this =
    inherit TheoryData<string, string list>()

    do this.Add("proProjector", [])
    do this.Add("proConsumer", [])
    do this.Add("trackingConsumer", [])
    do this.Add("summaryConsumer", [])
    do this.Add("proSync", [])

    do this.Add("proSync", ["--kafka"])
    do this.Add("proProjector", ["--kafka"])
    do this.Add("proProjector", ["--kafka"; "--parallelOnly"])
    do this.Add("proReactor", [])
    do this.Add("proReactor", ["--filter"])

    do for source in ["multiSource"; (* <-default *) "kafkaEventSpans"; "changeFeedOnly"] do
        for opts in [ []; ["--blank"]; ["--kafka"]; ["--kafka"; "--blank"] ] do
            do this.Add("proReactor", ["--source " + source; ] @ opts)

    do for t in ["eqxweb"; "eqxwebcs"] do
        this.Add(t, ["--todos"; "--cosmos"])
#if !DEBUG
        this.Add(t, ["--todos"])
        this.Add(t, ["--todos"; "--eventStore"])
    do this.Add("proSync", ["--marveleqx"])
#endif
    do this.Add("eqxTestbed", [])
    do this.Add("eqxShipping", [])

type DotnetBuild(output : ITestOutputHelper, folder : EquinoxTemplatesFixture) =

    let [<Theory; ClassData(typeof<TemplatesToTest>)>] ``dotnet runs`` (template, args) =
        output.WriteLine(sprintf "using %s" folder.PackagePath)
        let folder = Dir.cleared template
        Dotnet.instantiate folder template args
        Dotnet.build [folder]

    interface IClassFixture<EquinoxTemplatesFixture>

module Dummy = let [<EntryPoint>] main argv = 0