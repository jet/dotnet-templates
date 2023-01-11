module Reactor.Program

open Infrastructure
open Serilog
open System

module Config = Domain.Config

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-t"; Unique>]   TimeoutS of float
        
        | [<AltCommandLine "-i"; Unique>]   IdleDelayMs of int
        | [<AltCommandLine "-W"; Unique>]   WakeForResults

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."

                | IdleDelayMs _ ->          "Idle delay for scheduler. Default 1000ms"
                | WakeForResults _ ->       "Wake for all results to provide optimal throughput"

                | Dynamo _ ->               "specify DynamoDB input parameters"
    and Arguments(c : SourceArgs.Configuration, p : ParseResults<Parameters>) =
        let processorName =                 p.GetResult ProcessorName
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 16)
        let maxConcurrentProcessors =       p.GetResult(MaxWriters, 8)
        member val Verbose =                p.Contains Parameters.Verbose
        member val ProcessManagerMaxDop =   4
        member val CacheSizeMb =            10
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 10.
        member val PurgeInterval =          TimeSpan.FromHours 1.
        member val IdleDelay =              p.GetResult(IdleDelayMs, 1000) |> TimeSpan.FromMilliseconds
        member val WakeForResults =         p.Contains WakeForResults
        member x.ProcessorParams() =        Log.Information("Watching... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            processorName, maxReadAhead, maxConcurrentProcessors)
                                            (processorName, maxReadAhead, maxConcurrentProcessors)
        member val ProcessingTimeout =      p.GetResult(TimeoutS, 10.) |> TimeSpan.FromSeconds
        member val Store : Choice<SourceArgs.Dynamo.Arguments, unit> =
                                            match p.GetSubCommand() with
                                            | Dynamo a -> Choice1Of2 <| SourceArgs.Dynamo.Arguments(c, a)
                                            | a ->        Args.missingArg $"Unexpected Store subcommand %A{a}"
        member x.ConnectStoreAndSource(appName) : Config.Store * (ILogger -> string -> SourceConfig) * (ILogger -> unit) =
            let cache = Equinox.Cache (appName, sizeMb = x.CacheSizeMb)
            match x.Store with
            | Choice1Of2 a ->
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let indexStore, startFromTail, batchSizeCutoff, tailSleepInterval, streamsDop = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, cache)
                    let load = DynamoLoadModeConfig.Hydrate (context, streamsDop)
                    SourceConfig.Dynamo (indexStore, checkpoints, load, startFromTail, batchSizeCutoff, tailSleepInterval, x.StatsInterval)
                let store = Config.Store.Dynamo (context, cache)
                store, buildSourceConfig, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
            | Choice2Of2 a -> invalidOp "unexpected"

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(SourceArgs.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "Reactor"

let build (args : Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let store, buildSourceConfig, dumpMetrics = args.ConnectStoreAndSource(AppName)
    let log = Log.Logger
    let sink =
        let stats = Handler.Stats(log, args.StatsInterval, args.StateInterval, dumpMetrics)
        let handle = Handler.create store
        Handler.Config.StartSink(log, stats, handle, maxReadAhead, maxConcurrentStreams,
                                 wakeForResults = args.WakeForResults, idleDelay = args.IdleDelay, purgeInterval = args.PurgeInterval)
    let source, _awaitReactions =
        let sourceConfig = buildSourceConfig log consumerGroupName
        Handler.Config.StartSource(log, sink, sourceConfig)
    sink, source

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let run args =
    let sink, source = build args
    [|   Async.AwaitKeyboardInterruptAsTaskCanceledException()
         source.AwaitWithStopOnCancellation()
         sink.AwaitWithStopOnCancellation()
    |] |> Async.Parallel |> Async.Ignore<unit array>

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose = args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? Args.MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
