module Shipping.Watchdog.Program

open Propulsion.CosmosStore
open Serilog
open System

exception MissingArg of message : string with override this.Message = this.message

type Configuration(tryGet) =

    let get key =
        match tryGet key with
        | Some value -> value
        | None -> raise (MissingArg (sprintf "Missing Argument/Environment Variable %s" key))

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-t"; Unique>]   TimeoutS of float

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."
                | Verbose ->                "request Verbose Logging. Default: off."
                | Cosmos _ ->               "specify CosmosDB parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val ConsumerGroupName =      a.GetResult ConsumerGroupName
        member val Verbose =                a.Contains Parameters.Verbose
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 16)
        member val MaxConcurrentStreams =   a.GetResult(MaxWriters, 8)
        member val ProcessManagerMaxDop =   4
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val ProcessingTimeout =      a.GetResult(TimeoutS, 10.) |> TimeSpan.FromSeconds
        member val Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Cosmos cosmos) -> CosmosSourceArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos")
        member x.MonitoringParams() =
            let srcC = x.Source
            let leases : Microsoft.Azure.Cosmos.Container = srcC.ConnectLeases()
            Log.Information("Watching... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxConcurrentStreams, x.MaxReadAhead)
            Log.Information("Monitoring Group {processorName} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                x.ConsumerGroupName, leases.Database.Id, leases.Id, Option.toNullable srcC.MaxDocuments)
            if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            Log.Information("ChangeFeed Lag stats interval {lagS:n0}s", let f = srcC.LagFrequency in f.TotalSeconds)
            let storeClient, monitored = srcC.ConnectStoreAndMonitored()
            (storeClient, monitored, leases, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency)

    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-md"; Unique>]  MaxDocuments of int
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
                | Verbose ->                "request Verbose Change Feed Processor Logging. Default: off."
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited"
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
        let discovery =                     a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult CosmosSourceParameters.ConnectionMode
        let timeout =                       a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSourceParameters.Retries, 1)
        let maxRetryWaitTime =              a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      a.TryGetResult CosmosSourceParameters.Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val ContainerId =            a.GetResult CosmosSourceParameters.Container
        member val Verbose =                a.Contains Verbose
        member x.MonitoredContainer() =     connector.ConnectMonitored(database, x.ContainerId)

        member val FromTail =               a.Contains CosmosSourceParameters.FromTail
        member val MaxDocuments =           a.TryGetResult MaxDocuments
        member val LagFrequency : TimeSpan = a.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member val LeaseContainerId =       a.TryGetResult CosmosSourceParameters.LeaseContainer
        member private _.ConnectLeases containerId = connector.CreateUninitialized(database, containerId)
        member x.ConnectLeases() =          match x.LeaseContainerId with
                                            | None ->    x.ConnectLeases(x.ContainerId + "-aux")
                                            | Some sc -> x.ConnectLeases(sc)
        member x.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(database, x.ContainerId)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "Watchdog"

let startWatchdog log (processingTimeout, stats : Handler.Stats) (maxReadAhead, maxConcurrentStreams) driveTransaction
    : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<_, _>> =
    let handle = Handler.handle processingTimeout driveTransaction
    Propulsion.Streams.StreamsProjector.Start(log, maxReadAhead, maxConcurrentStreams, handle, stats, stats.StatsInterval)

open Shipping.Domain

let createProcessManager maxDop (context, cache) =
    let transactions = FinalizationTransaction.Config.Cosmos.create (context, cache)
    let containers = Container.Config.Cosmos.create (context, cache)
    let shipments = Shipment.Config.Cosmos.create (context, cache)
    FinalizationWorkflow.Service(transactions, containers, shipments, maxDop=maxDop)

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

let build (args : Args.Arguments) =
    // Connect to the Target CosmosDB, wire to Process Manager
    let storeClient, monitored, leases, processorName, startFromTail, maxDocuments, lagFrequency = args.MonitoringParams()
    let context = CosmosStoreContext.create storeClient
    let processManager =
        let cache = Equinox.Cache(AppName, sizeMb=10)
        createProcessManager args.ProcessManagerMaxDop (context, cache)
    let watchdogSink =
        let stats = Handler.Stats(Log.Logger, statsInterval=args.StatsInterval, stateInterval=args.StateInterval)
        startWatchdog Log.Logger (args.ProcessingTimeout, stats) (args.MaxReadAhead, args.MaxConcurrentStreams) processManager.Pump
    let pipeline =
        let log = Log.ForContext<CosmosStoreSource>()
        use observer = CosmosStoreSource.CreateObserver(log, watchdogSink.StartIngester, Seq.collect Handler.transformOrFilter)
        CosmosStoreSource.Run(log, monitored, leases, processorName, observer, startFromTail, ?maxDocuments=maxDocuments, lagReportFreq=lagFrequency)
    watchdogSink, pipeline

let run args = async {
    let sink, pipeline = build args
    pipeline |> Async.Start
    return! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose, changeFeedProcessorVerbose=args.Source.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously
                0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1

