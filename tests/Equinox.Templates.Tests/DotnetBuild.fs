namespace Equinox.Templates.Tests

open Xunit
open Xunit.Abstractions

type ProProjector() as this =
    inherit TheoryData<string list>()

    do for source in ["cosmos"; (* <-default *) "eventStore"; "sqlStreamStore"] do
        let variants =
            if source <> "cosmos" then [ []; ["--kafka"] ]
            else
#if DEBUG
                    [ ["--kafka"] ]
#else
                    [ []; ["--kafka"]; ["--kafka"; "--parallelOnly"] ]
#endif
        for opts in variants do
            this.Add(["--source " + source] @ opts)

type ProReactor() as this =
    inherit TheoryData<string list>()

    do for source in ["multiSource"; (* <-default *) "kafkaEventSpans"; "changeFeedOnly"] do
        for opts in [ []; ["--blank"]; ["--kafka"]; ["--kafka"; "--blank"] ] do
            this.Add(["--source " + source] @ opts)

type EqxWebs() as this =
    inherit TheoryData<string, string list>()

    do for t in ["eqxweb"; "eqxwebcs"] do
        do this.Add(t, ["--todos"; "--cosmos"])
#if !DEBUG
        do this.Add(t, ["--todos"])
        do this.Add(t, ["--todos"; "--eventStore"])
#endif

type DotnetBuild(output : ITestOutputHelper, folder : EquinoxTemplatesFixture) =

    let run template args =
        output.WriteLine(sprintf "using %s" folder.PackagePath)
        let folder = Dir.cleared template
        Dotnet.instantiate folder template args
        Dotnet.build [folder]

    #if DEBUG // Use this one to trigger an individual test
    let [<Fact>] ``*pending*`` ()               = run "proReactor" ["--source changeFeedOnly"; "--kafka"]
    #endif

    let [<Fact>] eqxTestbed ()                  = run "eqxTestbed" []
    let [<Fact>] eqxShipping ()                 = run "eqxShipping" []
    [<ClassData(typeof<ProProjector>)>]
    let [<Theory>] proProjector args            = run "proProjector" args
    let [<Fact>] proConsumer ()                 = run "proConsumer" []
    let [<Fact>] trackingConsumer ()            = run "trackingConsumer" []
    let [<Fact>] summaryConsumer ()             = run "summaryConsumer" []
    let [<Fact>] proSync ()                     = run "proSync" []
    let [<Fact>] proSyncK ()                    = run "proSync" ["--kafka"]
#if !DEBUG
    let [<Fact>] ``proSync-marvelEqx`` ()       = run "proSync" ["--marveleqx"]
#endif

    [<ClassData(typeof<EqxWebs>)>]
    let [<Theory>] eqxweb (template, args)      = run template args

    [<ClassData(typeof<ProReactor>)>]
    let [<Theory>] proReactor args              = run "proReactor" args
    let [<Fact>] proReactorDefault ()           = run "proReactor" []
    let [<Fact>] proReactorFilter ()            = run "proReactor" ["--filter"]

    interface IClassFixture<EquinoxTemplatesFixture>

module Dummy = let [<EntryPoint>] main _argv = 0