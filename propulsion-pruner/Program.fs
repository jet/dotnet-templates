module PrunerTemplate.Program

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
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int

        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 8."
                | MaxWriters _ ->           "maximum number of concurrent writes to target. Default: 4."

                | Verbose ->                "request Verbose Logging. Default: off"
                | CfpVerbose ->             "request Verbose Change Feed Processor Logging. Default: off"
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off"

                | SrcCosmos _ ->            "Cosmos Archive parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val ConsumerGroupName =      a.GetResult ConsumerGroupName
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 8)
        member val MaxWriters =             a.GetResult(MaxWriters, 4)
        member val Verbose =                a.Contains Verbose
        member val CfpVerbose =             a.Contains CfpVerbose
        member val PrometheusPort =         a.TryGetResult PrometheusPort
        member val MetricsEnabled =         a.Contains PrometheusPort
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val Archive : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (SrcCosmos cosmos) -> (CosmosSourceArguments (c, cosmos))
            | _ -> raise (MissingArg "Must specify cosmos for Archive")
        member x.DeletionTarget = x.Archive.Target
        member x.MonitoringParams() =
            let srcC = x.Archive
            let leasesContainer : Microsoft.Azure.Cosmos.Container =
                let dstC : CosmosSinkArguments = srcC.Target
                match srcC.LeaseContainer, dstC.LeaseContainer with
                | None, None ->     srcC.ConnectLeases(srcC.Container + "-aux")
                | Some sc, None ->  srcC.ConnectLeases(sc)
                | None, Some dc ->  dstC.ConnectLeases(dc)
                | Some _, Some _ -> raise (MissingArg "LeaseContainerSource and LeaseContainerDestination are mutually exclusive - can only store in one database")
            Log.Information("Pruning... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxWriters, x.MaxReadAhead)
            Log.Information("Monitoring Group {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                x.ConsumerGroupName, leasesContainer.Database.Id, leasesContainer.Id, Option.toNullable srcC.MaxDocuments)
            if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            (leasesContainer, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency)
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
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

        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] DstCosmos of ParseResults<CosmosSinkParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: off"
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name within `Database` to apply the pruning to"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 5."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 30."

                | DstCosmos _ ->            "CosmosDb Pruning Target parameters."
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        let discovery =                     a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.GetResult(CosmosSourceParameters.ConnectionMode, Microsoft.Azure.Cosmos.ConnectionMode.Direct)
        let timeout =                       a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSourceParameters.Retries, 5)
        let maxRetryWaitTime =              a.GetResult(CosmosSourceParameters.RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, mode)
        member val Database =               a.TryGetResult CosmosSourceParameters.Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.GetResult CosmosSourceParameters.Container
        member x.MonitoredContainer() =     connector.ConnectMonitor(x.Database, x.Container)
        
        member val FromTail =               a.Contains CosmosSourceParameters.FromTail
        member val MaxDocuments =           a.TryGetResult MaxDocuments
        member val LagFrequency =           a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member val LeaseContainer =         a.TryGetResult CosmosSourceParameters.LeaseContainer
        member x.ConnectLeases containerId = connector.CreateUninitialized(x.Database, containerId)

        member val Target =
            match a.TryGetSubCommand() with
            | Some (DstCosmos cosmos) -> CosmosSinkArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Target")

    and [<NoEquality; NoComparison>] CosmosSinkParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a Container name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 0."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosSinkArguments(c : Configuration, a : ParseResults<CosmosSinkParameters>) =
        let discovery =                     a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.GetResult(ConnectionMode, Microsoft.Azure.Cosmos.ConnectionMode.Direct)
        let timeout =                       a.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(CosmosSinkParameters.Retries, 0)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, mode)

        member val Database =               a.TryGetResult Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.TryGetResult Container  |> Option.defaultWith (fun () -> c.CosmosContainer)
        member x.Connect() =                connector.Connect("DELETION Target", x.Database, x.Container)

        member val LeaseContainer =         a.TryGetResult LeaseContainer
        member x.ConnectLeases containerId = connector.CreateUninitialized(x.Database, containerId)


    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "PrunerTemplate"

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256 // default is 0
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

let build (args : Args.Arguments, log : ILogger, storeLog : ILogger) =
    let aux, leaseId, startFromTail, maxDocuments, lagFrequency = args.MonitoringParams()

    // NOTE - DANGEROUS - events submitted to this sink get DELETED from the supplied Context!
    let deletingEventsSink =
        let target = args.DeletionTarget
        if (target.Database, target.Container) = (args.Archive.Database, args.Archive.Container) then
            raise (MissingArg "Danger! Can not prune a target based on itself")
        let context = target.Connect() |> Async.RunSynchronously |> CosmosStoreContext.create
        let eventsContext = Equinox.CosmosStore.Core.EventsContext(context, storeLog)
        CosmosStorePruner.Start(Log.Logger, args.MaxReadAhead, eventsContext, args.MaxWriters, args.StatsInterval, args.StateInterval)
    let pipeline =
        let monitored = args.Archive.MonitoredContainer()
        use observer = CosmosStoreSource.CreateObserver(log.ForContext<CosmosStoreSource>(), deletingEventsSink.StartIngester, Seq.collect Handler.selectPrunable)
        CosmosStoreSource.Run(log, monitored, aux, leaseId, observer, startFromTail, ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
    deletingEventsSink, pipeline

// A typical app will likely have health checks etc, implying the wireup would be via `UseMetrics()` and thus not use this ugly code directly
let startMetricsServer port : IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() }

let run args = async {
    let log, storeLog = Log.ForContext<Propulsion.Streams.Scheduling.StreamSchedulingEngine>(), Log.ForContext<Equinox.CosmosStore.Core.EventsContext>()
    let sink, pipeline = build (args, log, storeLog)
    pipeline |> Async.Start
    use _metricsServer : IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    do! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        let appName = sprintf "pruner:%s" args.ConsumerGroupName
        let appName = appName.Replace("ruben","")
        try Log.Logger <- LoggerConfiguration().Configure(appName, args.ConsumerGroupName, args.Verbose, args.CfpVerbose, args.MetricsEnabled).CreateLogger()
            try run args |> Async.RunSynchronously
                0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
