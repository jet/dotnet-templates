module ReactorTemplate.Program

open Equinox.EventStoreDb
open Equinox.SqlStreamStore
open Infrastructure
open Serilog
open System

type Configuration(tryGet) =
    inherit SourceArgs.Configuration(tryGet)

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
#if sourceKafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<SourceArgs.Kafka.Parameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<SourceArgs.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Esdb of ParseResults<SourceArgs.Esdb.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] SqlMs of ParseResults<SourceArgs.Sss.Parameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
#if sourceKafka
                | Kafka _ ->                "specify Kafka input parameters."
#else                
                | Cosmos _ ->               "specify CosmosDB input parameters."
                | Dynamo _ ->               "specify DynamoDB input parameters."
                | Esdb _ ->                 "specify EventStore input parameters."
                | SqlMs _ ->                "specify SqlStreamStore input parameters."
#endif
    and Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let processorName =                 p.GetResult ProcessorName
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 16)
        let maxConcurrentStreams =          p.GetResult(MaxWriters, 8)
        let cacheSizeMb =                   10
        member val Verbose =                p.Contains Verbose
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val PurgeInterval =          TimeSpan.FromHours 1.
        
        member _.ProcessorParams() =        Log.Information("Reacting... {processorName}, reading {maxReadAhead} ahead, {dop} streams",
                                                            processorName, maxReadAhead, maxConcurrentStreams)
                                            (processorName, maxReadAhead, maxConcurrentStreams)
#if sourceKafka
        member _.ConnectStoreAndSource(appName) : _ * _ * Args.KafkaSinkArguments * (string -> FsKafka.KafkaConsumerConfig) * (ILogger -> unit) =
            let p =
                match p.GetSubCommand() with
                | Kafka p -> SourceArgs.Kafka.Arguments(c, p)
                | p -> Args.missingArg $"Unexpected Source subcommand %A{p}"
            let createConsumerConfig groupName =
                FsKafka.KafkaConsumerConfig.Create(
                    appName, p.Broker, [p.Topic], groupName, Confluent.Kafka.AutoOffsetReset.Earliest,
                    maxInFlightBytes = p.MaxInFlightBytes, ?statisticsInterval = p.LagFrequency)
#if kafka && blank
            let targetStore = () in targetStore, targetStore, p.Kafka, createConsumerConfig, ignore
#else
            let cache = Equinox.Cache (appName, sizeMb = cacheSizeMb)
            let targetStore = p.ConnectTarget cache
            targetStore, targetStore, p.Kafka, createConsumerConfig, fun log ->
                Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
#endif                        
        member val VerboseStore =           false
#else            
        member val Store : Choice<SourceArgs.Cosmos.Arguments, SourceArgs.Dynamo.Arguments, SourceArgs.Esdb.Arguments, SourceArgs.Sss.Arguments> =
                                            match p.GetSubCommand() with
                                            | Cosmos p -> Choice1Of4 <| SourceArgs.Cosmos.Arguments(c, p)
                                            | Dynamo p -> Choice2Of4 <| SourceArgs.Dynamo.Arguments(c, p)
                                            | Esdb p ->   Choice3Of4 <| SourceArgs.Esdb.Arguments(c, p)
                                            | SqlMs p ->  Choice4Of4 <| SourceArgs.Sss.Arguments(c, p)
                                            | p ->        Args.missingArg $"Unexpected Store subcommand %A{p}"
        member x.VerboseStore =             match x.Store with
                                            | Choice1Of4 s -> s.Verbose
                                            | Choice2Of4 s -> s.Verbose
                                            | Choice3Of4 s -> s.Verbose
                                            | Choice4Of4 s -> false
        member x.ConnectStoreAndSource(appName) : Config.Store * _ * _ * (ILogger -> string -> SourceConfig) * (ILogger -> unit) =
            let cache = Equinox.Cache (appName, sizeMb = cacheSizeMb)
            match x.Store with
            | Choice1Of4 a ->
                let client, monitored = a.ConnectStoreAndMonitored()
                let buildSourceConfig log groupName =
                    let leases, startFromTail, maxItems, tailSleepInterval, lagFrequency = a.MonitoringParams(log)
                    let checkpointConfig = CosmosFeedConfig.Persistent (groupName, startFromTail, maxItems, lagFrequency)
                    SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval)
                let context = client |> CosmosStoreContext.create
                let store = Config.Store.Cosmos (context, cache)
#if kafka
                let kafka = a.Kafka
#if blank
                let targetStore = store
#else                
                let targetStore = a.ConnectTarget(cache)
#endif
#else
                let kafka, targetStore = (), a.ConnectTarget(cache)
#endif
                store, targetStore, kafka, buildSourceConfig, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Choice2Of4 a ->
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let indexStore, startFromTail, batchSizeCutoff, tailSleepInterval, streamsDop = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, cache)
                    let load = DynamoLoadModeConfig.Hydrate (context, streamsDop)
                    SourceConfig.Dynamo (indexStore, checkpoints, load, startFromTail, batchSizeCutoff, tailSleepInterval, x.StatsInterval)
                let store = Config.Store.Dynamo (context, cache)
#if kafka
                let kafka = a.Kafka
#if blank
                let targetStore = store
#else                
                let targetStore = a.ConnectTarget(cache)
#endif
#else
                let kafka, targetStore = (), a.ConnectTarget(cache)
#endif
                store, targetStore, kafka, buildSourceConfig, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
            | Choice3Of4 a ->
                let connection = a.Connect(Log.Logger, appName, EventStore.Client.NodePreference.Leader)
                let context = EventStoreContext connection
                let store = Config.Store.Esdb (context, cache)
                let targetStore = a.ConnectTarget(cache)
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, targetStore)
                    let hydrateBodies = true
                    SourceConfig.Esdb (connection.ReadConnection, checkpoints, hydrateBodies, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
#if kafka
                let kafka = a.Kafka
#else
                let kafka = ()
#endif
                store, targetStore, kafka, buildSourceConfig, fun log ->
                    Equinox.EventStoreDb.Log.InternalMetrics.dump log
                    Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                    Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
            | Choice4Of4 a ->
                let connection = a.Connect()
                let context = SqlStreamStoreContext connection
                let store = Config.Store.Sss (context, cache)
                let targetStore = a.ConnectTarget(cache)
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStoreSql(groupName)
                    let hydrateBodies = true
                    SourceConfig.Sss (connection.ReadConnection, checkpoints, hydrateBodies, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
#if kafka
                let kafka = a.Kafka
#else
                let kafka = ()
#endif
                store, targetStore, kafka, buildSourceConfig, fun log ->
                    Equinox.SqlStreamStore.Log.InternalMetrics.dump log
                    Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                    Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ReactorTemplate"

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let build (args : Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
#if sourceKafka
    let store, targetStore, sinkParams, createConsumerConfig, dumpMetrics = args.ConnectStoreAndSource(AppName)
#else
    let store, targetStore, sinkParams, buildSourceConfig, dumpMetrics = args.ConnectStoreAndSource(AppName)
#endif
    let log = Log.Logger

    (* ESTABLISH stats, handle *)
    
#if kafka // kafka 
    let broker, topic = sinkParams.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, Confluent.Kafka.Acks.All, topic)
    let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
        producer.Produce(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x)
    let dumpMetrics log =
        dumpMetrics log
        producer.DumpStats log
    let stats = Handler.Stats(log, args.StatsInterval, args.StateInterval, args.VerboseStore, dumpMetrics)
#if blank // kafka && blank
    let handle = Handler.handle produceSummary
#else // kafka && !blank
    let srcService = Todo.Config.create store
    let handle = Handler.handle srcService produceSummary
#endif // kafka && !blank
#else // !kafka (i.e., ingester)
#if blank // !kafka && blank
    // TODO: establish any relevant inputs, or re-run without `--blank` for example wiring code
    let handle = Ingester.handle
    let stats = Ingester.Stats(log, args.StatsInterval, args.StateInterval, args.VerboseStore, dumpMetrics)
#else // !kafka && !blank
    let srcService = Todo.Config.create store
    let dstService = TodoSummary.Config.create targetStore
    let handle = Ingester.handle srcService dstService
    let stats = Ingester.Stats(log, args.StatsInterval, args.StateInterval, args.VerboseStore, dumpMetrics)
#endif // blank
#endif

    (* ESTABLISH sink; AWAIT *)

#if sourceKafka
    let parseStreamEvents (res : Confluent.Kafka.ConsumeResult<_, _>) : seq<Propulsion.Streams.StreamEvent<_>> =
        Propulsion.Codec.NewtonsoftJson.RenderedSpan.parse res.Message.Value
    let consumerConfig = createConsumerConfig consumerGroupName
    let pipeline = 
        Propulsion.Kafka.StreamsConsumer.Start
            (   Log.Logger, consumerConfig, parseStreamEvents, handle, maxConcurrentStreams,
                stats = stats, statsInterval = args.StateInterval)
    [|  pipeline.AwaitWithStopOnCancellation()
#else
    let sink =
#if kafka // kafka 
#if blank // kafka && blank
        Handler.Config.StartSink(log, stats, handle, maxReadAhead, maxConcurrentStreams, purgeInterval = args.PurgeInterval)
#else // kafka && !blank
        Propulsion.Streams.Sync.StreamsSync.Start(
            Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, args.StatsInterval,
            Propulsion.Streams.Default.jsonSize, Propulsion.Streams.Default.eventSize)
#endif // kafka && !blank
#else // !kafka (i.e., ingester)
        Handler.Config.StartSink(log, stats, handle, maxReadAhead, maxConcurrentStreams, purgeInterval = args.PurgeInterval)
#endif // !kafka
    let source, _awaitReactions =
        let sourceConfig = buildSourceConfig log consumerGroupName
        Handler.Config.StartSource(log, sink, sourceConfig)
        
    [|  source.AwaitWithStopOnCancellation()
        sink.AwaitWithStopOnCancellation()
#endif
        Async.AwaitKeyboardInterruptAsTaskCanceledException() |]

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try build args |> Async.Parallel |> Async.Ignore<unit array> |> Async.RunSynchronously; 0
            with e when not (e :? Args.MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
