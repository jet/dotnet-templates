module Shipping.Watchdog.Program

open Serilog
open System

let [<Literal>] REGION =                    "EQUINOX_DYNAMO_REGION"
let [<Literal>] SERVICE_URL =               "EQUINOX_DYNAMO_SERVICE_URL"
let [<Literal>] ACCESS_KEY =                "EQUINOX_DYNAMO_ACCESS_KEY_ID"
let [<Literal>] SECRET_KEY =                "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
let [<Literal>] TABLE =                     "EQUINOX_DYNAMO_TABLE"
let [<Literal>] INDEX_TABLE =               "EQUINOX_DYNAMO_TABLE_INDEX"

exception MissingArg of message : string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

type Configuration(tryGet : string -> string option) =

    let get key =                           match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"

    member _.DynamoRegion =                 tryGet REGION
    member _.DynamoServiceUrl =             get SERVICE_URL
    member _.DynamoAccessKey =              get ACCESS_KEY
    member _.DynamoSecretKey =              get SECRET_KEY
    member _.DynamoTable =                  get TABLE
    member x.DynamoIndexTable =             tryGet INDEX_TABLE

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

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<CosmosSourceParameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<DynamoSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."

                | IdleDelayMs _ ->          "Idle delay for scheduler. Default 1000ms"
                | WakeForResults _ ->       "Wake for all results to provide optimal throughput"

                | Cosmos _ ->               "specify CosmosDB parameters."
                | Dynamo _ ->               "specify DynamoDB input parameters"
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        let maxReadAhead =                  a.GetResult(MaxReadAhead, 16)
        let maxConcurrentProcessors =       a.GetResult(MaxWriters, 8)
        member val Verbose =                a.Contains Parameters.Verbose
        member val ProcessorName =          a.GetResult ProcessorName
        member val ProcessManagerMaxDop =   4
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val PurgeInterval =          TimeSpan.FromHours 1.
        member val IdleDelay =              a.GetResult(IdleDelayMs, 1000) |> TimeSpan.FromMilliseconds
        member val WakeForResults =         a.Contains WakeForResults
        member x.ProcessorParams() =        Log.Information("Watching... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
                                            (x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
        member val ProcessingTimeout =      a.GetResult(TimeoutS, 10.) |> TimeSpan.FromSeconds
        // member val Cosmos =                 CosmosSourceArguments(c, a.GetResult Cosmos)
        member val Store : Choice<CosmosSourceArguments, DynamoSourceArguments> =
                                            match a.GetSubCommand() with
                                            | Cosmos a -> Choice1Of2 <| CosmosSourceArguments(c, a)
                                            | Dynamo a -> Choice2Of2 <| DynamoSourceArguments(c, a)
                                            | a -> missingArg $"Unexpected Store subcommand %A{a}"
        member x.ConnectStoreAndSource(cache) : Shipping.Domain.Config.Store<_> * (ILogger -> string -> SourceConfig) * (ILogger -> unit) =
            match x.Store with
            | Choice1Of2 cosmos ->
                let client, monitored = cosmos.ConnectStoreAndMonitored()
                let buildSourceConfig log groupName =
                    let leases, startFromTail, maxItems, lagFrequency = cosmos.MonitoringParams(log)
                    let checkpointConfig = CosmosCheckpointConfig.Persistent (groupName, startFromTail, maxItems, lagFrequency)
                    SourceConfig.Cosmos (monitored, leases, checkpointConfig)
                let context = client |> CosmosStoreContext.create
                Shipping.Domain.Config.Store.Cosmos (context, cache), buildSourceConfig, Equinox.CosmosStore.Core.Log.InternalMetrics.dump
            | Choice2Of2 dynamo ->
                let context = dynamo.Connect()
                let buildSourceConfig log consumerGroupName =
                    let indexStore, checkpoints, startFromTail, batchSizeCutoff, streamsDop = dynamo.MonitoringParams(log, consumerGroupName, cache)
                    let load = DynamoLoadModeConfig.Hydrate (context, streamsDop)
                    SourceConfig.Dynamo (indexStore, checkpoints, load, startFromTail, batchSizeCutoff, x.StatsInterval)
                Shipping.Domain.Config.Store.Dynamo (context, cache), buildSourceConfig, Equinox.DynamoStore.Core.Log.InternalMetrics.dump
        member x.StoreVerbose =             false
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-mi"; Unique>]  MaxItems of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Unique>]   Container of string // Actually Mandatory, but stating that is not supported
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: 1"
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name within `Database`"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        let discovery =                     a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult ConnectionMode
        let timeout =                       a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSourceParameters.Retries, 9)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   a.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        let leaseContainerId =              a.GetResult(LeaseContainer, containerId + "-aux")
        let fromTail =                      a.Contains CosmosSourceParameters.FromTail
        let maxItems =                      a.TryGetResult CosmosSourceParameters.MaxItems
        let lagFrequency =                  a.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member private _.ConnectLeases() =  connector.CreateUninitialized(database, leaseContainerId)
        member x.MonitoringParams(log : ILogger) =
            let leases : Microsoft.Azure.Cosmos.Container = x.ConnectLeases()
            log.Information("ChangeFeed Leases Database {db} Container {container}. MaxItems limited to {maxItems}",
                leases.Database.Id, leases.Id, Option.toNullable maxItems)
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            (leases, fromTail, maxItems, lagFrequency)
        member _.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(database, containerId)
    and [<NoEquality; NoComparison>] DynamoSourceParameters =
        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-s">]           ServiceUrl of string
        | [<AltCommandLine "-sa">]          AccessKey of string
        | [<AltCommandLine "-ss">]          SecretKey of string
        | [<AltCommandLine "-t">]           Table of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesTimeoutS of float
        | [<AltCommandLine "-i">]           IndexTable of string
        | [<AltCommandLine "-is">]          IndexSuffix of string
        | [<AltCommandLine "-mi"; Unique>]  MaxItems of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-d">]           StreamsDop of int
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                "Include low level Store logging."
                | ServiceUrl _ ->           "specify a server endpoint for a Dynamo account. (optional if environment variable " + SERVICE_URL + " specified)"
                | AccessKey _ ->            "specify an access key id for a Dynamo account. (optional if environment variable " + ACCESS_KEY + " specified)"
                | SecretKey _ ->            "specify a secret access key for a Dynamo account. (optional if environment variable " + SECRET_KEY + " specified)"
                | Retries _ ->              "specify operation retries (default: 9)."
                | RetriesTimeoutS _ ->      "specify max wait-time including retries in seconds (default: 60)"
                | Table _ ->                "specify a table name for the primary store. (optional if environment variable " + TABLE + " specified)"
                | IndexTable _ ->           "specify a table name for the index store. (optional if environment variable " + INDEX_TABLE + " specified. default: `Table`+`IndexSuffix`)"
                | IndexSuffix _ ->          "specify a suffix for the index store. (optional if environment variable " + INDEX_TABLE + " specified. default: \"-index\")"
                | MaxItems _ ->             "maximum events to load in a batch. Default: 100"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | StreamsDop _ ->           "parallelism when loading events from Store Feed Source. Default 4"
    and DynamoSourceArguments(c : Configuration, a : ParseResults<DynamoSourceParameters>) =
        let serviceUrl =                    a.TryGetResult ServiceUrl |> Option.defaultWith (fun () -> c.DynamoServiceUrl)
        let accessKey =                     a.TryGetResult AccessKey  |> Option.defaultWith (fun () -> c.DynamoAccessKey)
        let secretKey =                     a.TryGetResult SecretKey  |> Option.defaultWith (fun () -> c.DynamoSecretKey)
        let table =                         a.TryGetResult Table      |> Option.defaultWith (fun () -> c.DynamoTable)
        let indexSuffix =                   a.GetResult(IndexSuffix, "-index")
        let indexTable =                    a.TryGetResult IndexTable |> Option.orElseWith  (fun () -> c.DynamoIndexTable) |> Option.defaultWith (fun () -> table + indexSuffix)
        let maxItems =                      a.GetResult(MaxItems, 100)
        let fromTail =                      a.Contains FromTail
        let streamsDop =                    a.GetResult(StreamsDop, 4)
        let timeout =                       a.GetResult(RetriesTimeoutS, 60.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(Retries, 9)
        let connector =                     Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let client =                        connector.CreateClient()
        let indexStoreClient =              lazy client.ConnectStore("Index", indexTable)
        member val Verbose =                a.Contains Verbose
        member _.Connect() =                connector.LogConfiguration()
                                            client.ConnectStore("Main", table) |> DynamoStoreContext.create
        member _.MonitoringParams(log : ILogger, consumerGroupName, cache) =
            log.Information("DynamoStoreSource MaxItems {maxItems} Hydrater parallelism {streamsDop}", maxItems, streamsDop)
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            let indexStoreClient = indexStoreClient.Value
            let checkpoints = indexStoreClient.CreateCheckpointService(consumerGroupName, cache)
            
            indexStoreClient, checkpoints, fromTail, maxItems, streamsDop

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "Watchdog"

let build (args : Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let cache = Equinox.Cache(AppName, sizeMb = 10)
    let store, buildSourceConfig, dumpMetrics = args.ConnectStoreAndSource cache
    let log = Log.Logger
    let sink =
        let stats = Handler.Stats(log, args.StatsInterval, args.StateInterval, args.StoreVerbose, dumpMetrics)
        let manager = Shipping.Domain.FinalizationProcess.Config.create args.ProcessManagerMaxDop store
        Handler.Config.StartSink(log, stats, manager, args.ProcessingTimeout, maxReadAhead, maxConcurrentStreams,
                                 wakeForResults = args.WakeForResults, idleDelay = args.IdleDelay, purgeInterval = args.PurgeInterval)
    let source =
        let sourceConfig = buildSourceConfig log consumerGroupName
        Handler.Config.StartSource(log, sink, sourceConfig)
    sink, source

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCancelledException

let run args =
    let sink, source = build args
    [   Async.AwaitKeyboardInterruptAsTaskCancelledException()
        source.AwaitWithStopOnCancellation()
        sink.AwaitWithStopOnCancellation() ]
    |> Async.Parallel |> Async.Ignore<unit array>

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose = args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? MissingArg) && not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
