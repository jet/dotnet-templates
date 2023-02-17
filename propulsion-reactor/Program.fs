module ReactorTemplate.Program

#if !sourceKafka
open Equinox.EventStoreDb
open Equinox.SqlStreamStore
#endif
open Serilog
open System

type Configuration(tryGet) =
    inherit SourceArgs.Configuration(tryGet)

module Args =

    [<NoComparison; NoEquality>]
    type Source =
#if sourceKafka
        | Kafka of SourceArgs.Kafka.Arguments
        member _.VerboseStore = false
#else
        | Cosmos of SourceArgs.Cosmos.Arguments
        | Dynamo of SourceArgs.Dynamo.Arguments
        | Esdb of SourceArgs.Esdb.Arguments
        | SqlMs of SourceArgs.Sss.Arguments
        member x.VerboseStore =
            match x with
            | Cosmos s -> s.Verbose
            | Dynamo s -> s.Verbose
            | Esdb s -> s.Verbose
            | SqlMs s -> false
#endif

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
#if kafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<KafkaSinkParameters>
#else
#if     sourceKafka // && kafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<SourceArgs.Kafka.Parameters>
#else   // kafka && !sourceKafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<SourceArgs.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Esdb of ParseResults<SourceArgs.Esdb.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] SqlMs of ParseResults<SourceArgs.Sss.Parameters>
#endif
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
#if kafka                
                | Kafka _ ->                "Kafka Sink parameters."
#else                
#if     sourceKafka // && kafka
                | Kafka _ ->                "specify Kafka input parameters."
#else                
                | Cosmos _ ->               "specify CosmosDB input parameters."
                | Dynamo _ ->               "specify DynamoDB input parameters."
                | Esdb _ ->                 "specify EventStore input parameters."
                | SqlMs _ ->                "specify SqlStreamStore input parameters."
#endif
#endif
    and Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let processorName =                 p.GetResult ProcessorName
        let maxReadAhead =                  p.GetResult(MaxReadAhead, 16)
        let maxConcurrentStreams =          p.GetResult(MaxWriters, 8)
        let cacheSizeMb =                   10
        member val Verbose =                p.Contains Verbose
        member x.VerboseStore =             x.Source.VerboseStore           
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val PurgeInterval =          TimeSpan.FromHours 1.
        
        member _.ProcessorParams() =        Log.Information("Reacting... {processorName}, reading {maxReadAhead} ahead, {dop} streams",
                                                            processorName, maxReadAhead, maxConcurrentStreams)
                                            (processorName, maxReadAhead, maxConcurrentStreams)
#if sourceKafka
        member x.ConnectStoreAndSource(appName) : _ * _ * _ * (string -> FsKafka.KafkaConsumerConfig) * (ILogger -> unit) =
            let (Source.Kafka a) = x.Source
            let createConsumerConfig groupName =
                FsKafka.KafkaConsumerConfig.Create(
                    appName, a.Broker, [a.Topic], groupName, Confluent.Kafka.AutoOffsetReset.Earliest,
                    maxInFlightBytes = a.MaxInFlightBytes, ?statisticsInterval = a.LagFrequency)
#if (kafka && blank)
            let targetStore = () in targetStore, targetStore, x.Sink, createConsumerConfig, ignore
#else
            let cache = Equinox.Cache (appName, sizeMb = cacheSizeMb)
            let targetStore = a.ConnectTarget cache
            targetStore, targetStore, x.Sink, createConsumerConfig, fun log ->
                Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
#endif                       
#else            
        member x.ConnectStoreAndSource(appName) : Config.Store * _ * _ * (ILogger -> string -> SourceConfig) * (ILogger -> unit) =
            let cache = Equinox.Cache (appName, sizeMb = cacheSizeMb)
            match x.Source with
            | Source.Cosmos a ->
                let client, monitored = a.ConnectStoreAndMonitored()
                let buildSourceConfig log groupName =
                    let leases, startFromTail, maxItems, tailSleepInterval, lagFrequency = a.MonitoringParams(log)
                    let checkpointConfig = CosmosFeedConfig.Persistent (groupName, startFromTail, maxItems, lagFrequency)
                    SourceConfig.Cosmos (monitored, leases, checkpointConfig, tailSleepInterval)
                let context = client |> CosmosStoreContext.create
                let store = Config.Store.Cosmos (context, cache)
#if blank
                let targetStore = store
#else                
                let targetStore = a.ConnectTarget(cache)
#endif
                store, targetStore, x.Sink, buildSourceConfig, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Source.Dynamo a ->
                let context = a.Connect()
                let buildSourceConfig log groupName =
                    let indexStore, startFromTail, batchSizeCutoff, tailSleepInterval, streamsDop = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, cache)
                    let load = Propulsion.DynamoStore.WithData (streamsDop, context)
                    SourceConfig.Dynamo (indexStore, checkpoints, load, startFromTail, batchSizeCutoff, tailSleepInterval, x.StatsInterval)
                let store = Config.Store.Dynamo (context, cache)
#if blank
                let targetStore = store
#else                
                let targetStore = a.ConnectTarget(cache)
#endif
                store, targetStore, x.Sink, buildSourceConfig, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
            | Source.Esdb a ->
                let connection = a.Connect(appName, EventStore.Client.NodePreference.Leader)
                let context = EventStoreContext connection
                let store = Config.Store.Esdb (context, cache)
#if blank
                let targetStore = store
#else                
                let targetStore = a.ConnectTarget(cache)
#endif
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStore(groupName, targetStore)
                    let withData = true
                    SourceConfig.Esdb (connection.ReadConnection, checkpoints, withData, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
                store, targetStore, x.Sink, buildSourceConfig, fun log ->
                    Equinox.EventStoreDb.Log.InternalMetrics.dump log
                    Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                    Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
            | Source.SqlMs a ->
                let connection = a.Connect()
                let context = SqlStreamStoreContext connection
                let store = Config.Store.Sss (context, cache)
                let buildSourceConfig log groupName =
                    let startFromTail, maxItems, tailSleepInterval = a.MonitoringParams(log)
                    let checkpoints = a.CreateCheckpointStoreSql(groupName)
                    let withData = true
                    SourceConfig.Sss (connection.ReadConnection, checkpoints, withData, startFromTail, maxItems, tailSleepInterval, x.StatsInterval)
#if blank
                let targetStore = store
#else                
                let targetStore = a.ConnectTarget(cache)
#endif
                store, targetStore, x.Sink, buildSourceConfig, fun log ->
                    Equinox.SqlStreamStore.Log.InternalMetrics.dump log
                    Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
                    Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
#endif
#if (!kafka)
        member val Sink =                   ()
#if sourceKafka
        member val Source : Source =        match p.GetSubCommand() with
                                            | Kafka p -> Source.Kafka <| SourceArgs.Kafka.Arguments(c, p)
                                            | p -> Args.missingArg $"Unexpected Source subcommand %A{p}"
#else        
        member val Source : Source =        match p.GetSubCommand() with
                                            | Cosmos p -> Source.Cosmos <| SourceArgs.Cosmos.Arguments(c, p)
                                            | Dynamo p -> Source.Dynamo <| SourceArgs.Dynamo.Arguments(c, p)
                                            | Esdb p ->   Source.Esdb   <| SourceArgs.Esdb.Arguments(c, p)
                                            | SqlMs p ->  Source.SqlMs  <| SourceArgs.Sss.Arguments(c, p)
                                            | p ->        Args.missingArg $"Unexpected Source subcommand %A{p}"
#endif
#else // kafka                                            
        member val Sink =                   match p.GetSubCommand() with
                                            | Parameters.Kafka p -> KafkaSinkArguments(c, p)
                                            | p -> Args.missingArg $"Unexpected Sink subcommand %A{p}"
        member x.Source : Source =          x.Sink.Source

    and [<NoEquality; NoComparison>] KafkaSinkParameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
#if     sourceKafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<SourceArgs.Kafka.Parameters>
#else   
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<SourceArgs.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<SourceArgs.Dynamo.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Esdb of ParseResults<SourceArgs.Esdb.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] SqlMs of ParseResults<SourceArgs.Sss.Parameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
#if     sourceKafka
                | Kafka _ ->                "specify Kafka input parameters."
#else                
                | Cosmos _ ->               "specify CosmosDB input parameters."
                | Dynamo _ ->               "specify DynamoDB input parameters."
                | Esdb _ ->                 "specify EventStore input parameters."
                | SqlMs _ ->                "specify SqlStreamStore input parameters."
#endif

    and KafkaSinkArguments(c : Configuration, p : ParseResults<KafkaSinkParameters>) =
        member val Broker =                 p.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  p.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member x.BuildTargetParams() =      x.Broker, x.Topic
#if sourceKafka
        member val Source =                 match p.GetSubCommand() with
                                            | KafkaSinkParameters.Kafka p -> Source.Kafka <| SourceArgs.Kafka.Arguments(c, p)
                                            | p -> Args.missingArg $"Unexpected Source subcommand %A{p}"
#else        
        member val Source : Source =        match p.GetSubCommand() with
                                            | Cosmos p -> Source.Cosmos <| SourceArgs.Cosmos.Arguments(c, p)
                                            | Dynamo p -> Source.Dynamo <| SourceArgs.Dynamo.Arguments(c, p)
                                            | Esdb p ->   Source.Esdb   <| SourceArgs.Esdb.Arguments(c, p)
                                            | SqlMs p ->  Source.SqlMs  <| SourceArgs.Sss.Arguments(c, p)
                                            | p ->        Args.missingArg $"Unexpected Source subcommand %A{p}"
#endif
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
    
#if kafka
    let broker, topic = sinkParams.BuildTargetParams()
    let producer = Propulsion.Kafka.Producer(Log.Logger, AppName, broker, Confluent.Kafka.Acks.All, topic)
    let produceSummary (x : Propulsion.Codec.NewtonsoftJson.RenderedSummary) =
        producer.Produce(x.s, Propulsion.Codec.NewtonsoftJson.Serdes.Serialize x) |> Propulsion.Internal.Async.ofTask
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
#else // !sourceKafka
    let sink =
#if kafka // !sourceKafka && kafka 
#if blank // !sourceKafka && kafka && blank
        Handler.Config.StartSink(log, stats, handle, maxReadAhead, maxConcurrentStreams, purgeInterval = args.PurgeInterval)
#else // !sourceKafka && kafka && !blank
        Propulsion.Streams.Sync.StreamsSync.Start(
            Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, args.StatsInterval,
            Propulsion.Streams.Default.jsonSize, Propulsion.Streams.Default.eventSize)
#endif // !sourceKafka && kafka && !blank
#else // !sourceKafka && !kafka (i.e., ingester)
        Ingester.Config.StartSink(log, stats, handle, maxReadAhead, maxConcurrentStreams, purgeInterval = args.PurgeInterval)
#endif // !sourceKafka && !kafka
    let source, _awaitReactions =
        let sourceConfig = buildSourceConfig log consumerGroupName
#if kafka        
        Handler.Config.StartSource(log, sink, sourceConfig)
#else
        Ingester.Config.StartSource(log, sink, sourceConfig)
#endif
    [|  source.AwaitWithStopOnCancellation()
        sink.AwaitWithStopOnCancellation()
#endif // !sourceKafka
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
