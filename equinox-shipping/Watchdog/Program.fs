module Shipping.Watchdog.Program

open Serilog
open System

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

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<SourceArgs.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Esdb of ParseResults<SourceArgs.Esdb.Parameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."

                | IdleDelayMs _ ->          "Idle delay for scheduler. Default 1000ms"
                | WakeForResults _ ->       "Wake for all results to provide optimal throughput"

                | Cosmos _ ->               "specify CosmosDB parameters."
                | Dynamo _ ->               "specify DynamoDB input parameters"
                | Esdb _ ->                 "specify EventStore DB input parameters"
    and Arguments(c: SourceArgs.Configuration, p: ParseResults<Parameters>) =
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
        member val Store: Choice<SourceArgs.Cosmos.Arguments, SourceArgs.Dynamo.Arguments, SourceArgs.Esdb.Arguments> =
                                            match p.GetSubCommand() with
                                            | Cosmos a -> Choice1Of3 <| SourceArgs.Cosmos.Arguments(c, a)
                                            | Dynamo a -> Choice2Of3 <| SourceArgs.Dynamo.Arguments(c, a)
                                            | Esdb a ->   Choice3Of3 <| SourceArgs.Esdb.Arguments(c, a)
                                            | a ->        Args.missingArg $"Unexpected Store subcommand %A{a}"
        member x.VerboseStore =             match x.Store with
                                            | Choice1Of3 s -> s.Verbose
                                            | Choice2Of3 s -> s.Verbose
                                            | Choice3Of3 s -> s.Verbose
        member x.ConnectStoreAndSource(appName): Store.Config<_> * (ILogger -> string -> SourceConfig) * (ILogger -> unit) =
            let cache = Equinox.Cache (appName, sizeMb = x.CacheSizeMb)
            match x.Store with
            | Choice1Of3 a ->
                let context, monitored, leases = a.ConnectWithFeed() |> Async.RunSynchronously
                let buildSourceConfig _log groupName =
                    let startFromTail, maxItems, tailSleepInterval, lagFrequency = a.MonitoringParams
                    let checkpointConfig = CosmosFeedConfig.Persistent (groupName, startFromTail, maxItems, lagFrequency)
                    SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval)
                let store = Store.Config.Cosmos (context, cache)
                store, buildSourceConfig, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Choice2Of3 a ->
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let indexContext, startFromTail, batchSizeCutoff, tailSleepInterval, streamsDop = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, cache)
                    let load = Propulsion.DynamoStore.WithData (streamsDop, context)
                    SourceConfig.Dynamo (indexContext, checkpoints, load, startFromTail, batchSizeCutoff, tailSleepInterval, x.StatsInterval)
                let store = Store.Config.Dynamo (context, cache)
                store, buildSourceConfig, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
            | Choice3Of3 a ->
                let connection = a.Connect(Log.Logger, appName, EventStore.Client.NodePreference.Leader)
                let context = connection |> EventStoreContext.create
                let store = Store.Config.Esdb (context, cache)
                let targetStore = a.ConnectTarget(cache)
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, targetStore)
                    let withData = true
                    SourceConfig.Esdb (connection.ReadConnection, checkpoints, withData, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
                store, buildSourceConfig, Equinox.EventStoreDb.Log.InternalMetrics.dump

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(SourceArgs.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "Watchdog"

let build (args: Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let store, buildSourceConfig, dumpMetrics = args.ConnectStoreAndSource(AppName)
    let log = Log.Logger
    let sink =
        let stats = Handler.Stats(log, args.StatsInterval, args.StateInterval, args.VerboseStore, dumpMetrics)
        let manager = Shipping.Domain.FinalizationProcess.Factory.create args.ProcessManagerMaxDop store
        Handler.Factory.StartSink(log, stats, maxConcurrentStreams, manager, args.ProcessingTimeout, maxReadAhead, 
                                 wakeForResults = args.WakeForResults, idleDelay = args.IdleDelay, purgeInterval = args.PurgeInterval)
    let source, _awaitReactions =
        let sourceConfig = buildSourceConfig log consumerGroupName
        Handler.Factory.StartSource(log, sink, sourceConfig)
    sink, source

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let run args =
    let sink, source = build args
    [|   Async.AwaitKeyboardInterruptAsTaskCanceledException()
         source.AwaitWithStopOnCancellation()
         sink.AwaitWithStopOnCancellation()
    |] |> Async.Parallel |> Async.Ignore<unit[]>

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
