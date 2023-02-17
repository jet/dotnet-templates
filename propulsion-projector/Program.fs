module ProjectorTemplate.Program

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
#if kafka
        (* Kafka Args *)
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
#endif
// #if cosmos
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<SourceArgs.Cosmos.Parameters>
// #endif
#if dynamo
        | [<CliPrefix(CliPrefix.None); Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
#endif
#if esdb
        | [<CliPrefix(CliPrefix.None); Last>] Esdb of ParseResults<SourceArgs.Esdb.Parameters>
#endif
#if sss
        | [<CliPrefix(CliPrefix.None); Last>] SqlMs of ParseResults<SourceArgs.Sss.Parameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "Request Verbose Logging. Default: off"
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024"
#if kafka
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
#endif
// #if cosmos
                | Cosmos _ ->               "specify CosmosDb input parameters"
// #endif
#if dynamo
                | Dynamo _ ->               "specify DynamoDb input parameters"
#endif
#if esdb
                | Esdb _ ->                 "specify EventStore input parameters."
#endif
#if sss
                | SqlMs _ ->                "specify SqlStreamStore input parameters."
#endif
    and Arguments(c : SourceArgs.Configuration, p : ParseResults<Parameters>) =
        let processorName =                 p.GetResult ProcessorName
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 64)
        let maxConcurrentProcessors =       p.GetResult(MaxWriters, 1024)
        member val Verbose =                p.Contains Parameters.Verbose
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 10.
        member val CacheSizeMb =            10
        member _.ProcessorParams() =        Log.Information("Projecting... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            processorName, maxReadAhead, maxConcurrentProcessors)
                                            (processorName, maxReadAhead, maxConcurrentProcessors)
        member val Store =                  match p.GetSubCommand() with
// #if cosmos                                            
                                            | Cosmos p -> SourceArgs.Cosmos.Arguments(c, p)
// #endif                                            
#if dynamo                                            
                                            | Dynamo p -> SourceArgs.Dynamo.Arguments(c, p)
#endif                                            
#if esdb                                            
                                            | Esdb p ->   SourceArgs.Esdb.Arguments(c, p)
#endif                                            
#if sss                                            
                                            | SqlMs p ->  SourceArgs.Sss.Arguments(c, p)
#endif                                            
                                            | p ->        Args.missingArg $"Unexpected Store subcommand %A{p}"
        member x.VerboseStore =             x.Store.Verbose
#if kafka        
        member val Sink =                   KafkaSinkArguments(c, p)
#else
        member val Sink =                   ()
#endif        
        member x.ConnectSource(appName) : (ILogger -> string -> SourceConfig) * _ * (ILogger -> unit) =
            let cache = Equinox.Cache (appName, sizeMb = x.CacheSizeMb)
            match x.Store with
            | a ->
//#if cosmos            
                let monitored = a.ConnectMonitored()
                let buildSourceConfig log groupName =
                    let leases, startFromTail, maxItems, tailSleepInterval, lagFrequency = a.MonitoringParams(log)
                    let checkpointConfig = CosmosFeedConfig.Persistent (groupName, startFromTail, maxItems, lagFrequency)
                    SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval)
                buildSourceConfig, x.Sink, ignore
// #endif                
#if dynamo
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let indexStore, startFromTail, batchSizeCutoff, tailSleepInterval, streamsDop = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, cache)
                    let load = Propulsion.DynamoStore.WithData (streamsDop, context)
                    SourceConfig.Dynamo (indexStore, checkpoints, load, startFromTail, batchSizeCutoff, tailSleepInterval, x.StatsInterval)
                buildSourceConfig, x.Sink, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
#endif
#if esdb
                let connection = a.Connect(appName, EventStore.Client.NodePreference.Leader)
                let targetStore = a.ConnectTarget(cache)
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, targetStore)
                    let withData = true
                    SourceConfig.Esdb (connection.ReadConnection, checkpoints, withData, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
                buildSourceConfig, x.Sink, fun log ->
                    Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                    Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
#endif
#if sss
                let connection = a.Connect()
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStoreSql(groupName)
                    let withData = true
                    SourceConfig.Sss (connection.ReadConnection, checkpoints, withData, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
                buildSourceConfig, x.Sink, fun log ->
                    Equinox.SqlStreamStore.Log.InternalMetrics.dump log
                    Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                    Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
#endif
#if kafka
    
    and KafkaSinkArguments(c : SourceArgs.Configuration, p : ParseResults<Parameters>) =
        member val Broker =                 p.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  p.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member x.BuildTargetParams() =      x.Broker, x.Topic
#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(SourceArgs.Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ProjectorTemplate"

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let build (args : Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentProcessors = args.ProcessorParams()
    let buildSourceConfig, target, dumpMetrics = args.ConnectSource(AppName)
#if kafka // kafka
    let broker, topic = target.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, Confluent.Kafka.Acks.All, topic)
#if     parallelOnly // kafka && parallelOnly
    let sink = Propulsion.Kafka.ParallelProducerSink.Start(maxReadAhead, maxConcurrentProcessors, Handler.render, producer, args.StatsInterval)
#else // kafka && !parallelOnly
    let stats = Handler.ProductionStats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Kafka.StreamsProducerSink.Start(Log.Logger, maxReadAhead, maxConcurrentProcessors, Handler.render, producer, stats, statsInterval = args.StatsInterval)
#endif // kafka && !parallelOnly
#else // !kafka
    let stats = Handler.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Streams.Default.Config.Start(Log.Logger, maxReadAhead, maxConcurrentProcessors, Handler.handle, stats, args.StatsInterval)
#endif // !kafka
#if (cosmos && parallelOnly)
    // Custom logic for establishing the source, as we're not projecting StreamEvents - TODO could probably be generalized
    let source =
        let mapToStreamItems (x : System.Collections.Generic.IReadOnlyCollection<'a>) : seq<'a> = upcast x
        let observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Handler.mapToStreamItems)
        match buildSourceConfig Log.Logger consumerGroupName with SourceConfig.Cosmos (monitoredContainer, leasesContainer, checkpoints, tailSleepInterval : TimeSpan) ->
        match checkpoints with
        | Ephemeral _ -> failwith "Unexpected"
        | Persistent (processorName, startFromTail, maxItems, lagFrequency) ->

        Propulsion.CosmosStore.CosmosStoreSource.Start(Log.Logger, monitoredContainer, leasesContainer, consumerGroupName, observer,
                                                       startFromTail = startFromTail, ?maxItems=maxItems, lagReportFreq=lagFrequency)
#else
    let source, _awaitReactions =
        let sourceConfig = buildSourceConfig Log.Logger consumerGroupName
        Handler.Config.StartSource(Log.Logger, sink, sourceConfig)
#endif        
    [|  Async.AwaitKeyboardInterruptAsTaskCanceledException()
        source.AwaitWithStopOnCancellation()
        sink.AwaitWithStopOnCancellation() |]
    
let run args =
    build args |> Async.Parallel |> Async.Ignore<unit[]>

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? Args.MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
