module PrunerTemplate.Program

open Propulsion.CosmosStore
open Serilog
open System

exception MissingArg of message: string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

type Configuration(tryGet) =

    let get key = match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off"
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off"
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 8."
                | MaxWriters _ ->           "maximum number of concurrent writes to target. Default: 4."
                | SrcCosmos _ ->            "Cosmos Archive parameters."
    and Arguments(c: Configuration, p: ParseResults<Parameters>) =
        member val Verbose =                p.Contains Parameters.Verbose
        member val PrometheusPort =         p.TryGetResult PrometheusPort
        member val ProcessorName =          p.GetResult ProcessorName
        member val MaxReadAhead =           p.GetResult(MaxReadAhead, 8)
        member val MaxWriters =             p.GetResult(MaxWriters, 4)
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val Source: CosmosSourceArguments =
            match p.GetSubCommand() with
            | SrcCosmos cosmos -> CosmosSourceArguments(c, cosmos)
            | _ -> missingArg "Must specify cosmos for Source"
        member x.DeletionTarget =           x.Source.Target
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-b"; Unique>]   MaxItems of int
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
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Change Feed Processor Logging. Default: off"
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: 1"
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name within `Database` to apply the pruning to"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 5."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 30."

                | DstCosmos _ ->            "CosmosDb Pruning Target parameters."
    and CosmosSourceArguments(c: Configuration, p: ParseResults<CosmosSourceParameters>) =
        let discovery =                     p.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult CosmosSourceParameters.ConnectionMode
        let timeout =                       p.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(CosmosSourceParameters.Retries, 5)
        let maxRetryWaitTime =              p.GetResult(CosmosSourceParameters.RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let databaseId =                    p.TryGetResult CosmosSourceParameters.Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   p.GetResult CosmosSourceParameters.Container
       
        let fromTail =                      p.Contains CosmosSourceParameters.FromTail
        let maxItems =                      p.TryGetResult MaxItems
        let lagFrequency: TimeSpan =        p.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member val Verbose =                p.Contains CosmosSourceParameters.Verbose
        member val MonitoringParams =       fromTail, maxItems, lagFrequency
        member x.ConnectFeed() =            if x.Target.Is(databaseId, containerId) then missingArg "Danger! Can not prune a target based on itself"
                                            match x.Target.MaybeLeasesContainer with
                                            | Some leasesContainerInTarget -> connector.ConnectFeed(databaseId, containerId, leasesContainerInTarget)
                                            | None ->
                                                let leaseContainerId = p.GetResult(CosmosSourceParameters.LeaseContainer, containerId + "-aux")
                                                connector.ConnectFeed(databaseId, containerId, leaseContainerId)
        member val Target: CosmosSinkArguments =
            match p.GetSubCommand() with
            | DstCosmos cosmos -> CosmosSinkArguments(c, cosmos)
            | _ -> missingArg "Must specify cosmos for Target"
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
            member p.Usage = p |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a Container name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 0."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosSinkArguments(c: Configuration, p: ParseResults<CosmosSinkParameters>) =
        let discovery =                     p.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let timeout =                       p.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(CosmosSinkParameters.Retries, 0)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let databaseId =                    p.TryGetResult Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   p.TryGetResult Container  |> Option.defaultWith (fun () -> c.CosmosContainer)
        let leaseContainerId =              p.TryGetResult LeaseContainer
        member _.Is(d, c) =                 databaseId = d && containerId = c
        member _.Connect() = async {        let! context = connector.ConnectContext("DELETION Target", databaseId, containerId, tipMaxEvents = 256, ?auxContainerId = leaseContainerId)
                                            return Equinox.CosmosStore.Core.EventsContext(context, Store.log) }
        member _.MaybeLeasesContainer: Microsoft.Azure.Cosmos.Container option = leaseContainerId |> Option.map (fun id -> connector.LeasesContainer(databaseId, id))

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "PrunerTemplate"

let build (args: Args.Arguments, log: ILogger) =
    let archive = args.Source
    let processorName = args.ProcessorName
    // NOTE - DANGEROUS - events submitted to this sink get DELETED from the supplied Context!
    let deletingEventsSink =
        let target = args.DeletionTarget
        let eventsContext = target.Connect() |> Async.RunSynchronously
        let stats = CosmosStorePrunerStats(Log.Logger, args.StatsInterval, args.StateInterval)
        CosmosStorePruner.Start(Log.Logger, args.MaxReadAhead, eventsContext, args.MaxWriters, stats)
    let monitored, leases = archive.ConnectFeed() |> Async.RunSynchronously
    let source =
        let observer = CosmosStoreSource.CreateObserver(log.ForContext<CosmosStoreSource>(), deletingEventsSink.StartIngester, Seq.collect Handler.selectPrunable)
        let startFromTail, maxItems, lagFrequency = args.Source.MonitoringParams
        CosmosStoreSource.Start(log, monitored, leases, processorName, observer,
                                startFromTail = startFromTail, ?maxItems = maxItems, lagReportFreq = lagFrequency)
    deletingEventsSink, source

// A typical app will likely have health checks etc, implying the wireup would be via `endpoints.MapMetrics()` and thus not use this ugly code directly
let startMetricsServer port: IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() }

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let run (args: Args.Arguments) = async {
    let log = (Log.forGroup args.ProcessorName).ForContext<Propulsion.Sinks.Factory>()
    let sink, source = build (args, log)
    use _metricsServer: IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    return! [|   Async.AwaitKeyboardInterruptAsTaskCanceledException()
                 source.AwaitWithStopOnCancellation()
                 sink.AwaitWithStopOnCancellation()
            |] |> Async.Parallel |> Async.Ignore<unit[]>
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(AppName, args.Verbose, args.Source.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
