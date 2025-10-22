module ArchiverTemplate.Program

open Propulsion.CosmosStore
open Serilog
open System

let [<Literal>] CONNECTION =                "EQUINOX_COSMOS_CONNECTION"
let [<Literal>] DATABASE =                  "EQUINOX_COSMOS_DATABASE"
let [<Literal>] CONTAINER =                 "EQUINOX_COSMOS_CONTAINER"

type Configuration(tryGet) =

    let get key = match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get CONNECTION
    member _.CosmosDatabase =               get DATABASE
    member _.CosmosContainer =              get CONTAINER

module Args =

    open Argu
    [<NoEquality; NoComparison; RequireSubcommand>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-S"; Unique>]   SyncVerbose
        | [<AltCommandLine "-p"; Unique>]   PrometheusPort of int
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-t"; Unique>]   RuThreshold of float
        | [<AltCommandLine "-k"; Unique>]   MaxKib of int
        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos">] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging. Default: off"
                | SyncVerbose ->            "request Logging for Sync operations (Writes). Default: off"
                | PrometheusPort _ ->       "port from which to expose a Prometheus /metrics endpoint. Default: off"
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 32."
                | MaxWriters _ ->           "maximum number of concurrent writes to target permitted. Default: 4."
                | RuThreshold _ ->          "minimum request charge required to log. Default: 0"
                | MaxKib _ ->               "max KiB to submit to Sync operation. Default: 512"
                | SrcCosmos _ ->            "Cosmos input parameters."
    and Arguments(c: Configuration, p: ParseResults<Parameters>) =
        member val Verbose =                p.Contains Parameters.Verbose
        member val SyncLogging =            p.Contains SyncVerbose, p.TryGetResult RuThreshold
        member val PrometheusPort =         p.TryGetResult PrometheusPort
        member val ProcessorName =          p.GetResult ProcessorName
        member val MaxReadAhead =           p.GetResult(MaxReadAhead, 32)
        member val MaxWriters =             p.GetResult(MaxWriters, 4)
        member val MaxBytes =               p.GetResult(MaxKib, 512) * 1024
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val Source: CosmosSourceArguments =
            match p.GetSubCommand() with
            | SrcCosmos cosmos -> CosmosSourceArguments(c, cosmos)
            | _ -> p.Raise "Must specify cosmos for SrcCosmos"
        member x.DestinationArchive = x.Source.Archive
        member x.Parameters() =
            Log.Information("Archiving... {dop} writers, max {maxReadAhead} batches read ahead, max write batch {maxKib} KiB",
                            x.MaxWriters, x.MaxReadAhead, x.MaxBytes / 1024)
            x.ProcessorName, x.MaxReadAhead, x.MaxWriters, x.MaxBytes
        
    and [<NoEquality; NoComparison; RequireSubcommand>] CosmosSourceParameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-b"; Unique>]   MaxItems of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c"; Unique>]   Container of string // Actually Mandatory, but stating that is not supported
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float

        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos">] DstCosmos of ParseResults<CosmosSinkParameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Change Feed Processor Logging. Default: off"
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: 1"
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           $"specify a connection string for a Cosmos account. (optional if environment variable $%s{CONNECTION} specified)"
                | Database _ ->             $"specify a database name for store. (optional if environment variable $%s{DATABASE} specified)"
                | Container _ ->            $"specify a container name for store. (optional if environment variable $%s{CONTAINER} specified)"
                | Retries _ ->              "specify operation retries. Default: 5."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 30."

                | DstCosmos _ ->            "CosmosDb Sink parameters."
    and CosmosSourceArguments(c: Configuration, p: ParseResults<CosmosSourceParameters>) =
        let discovery =                     p.GetResult(CosmosSourceParameters.Connection, fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult CosmosSourceParameters.ConnectionMode
        let retries =                       p.GetResult(CosmosSourceParameters.Retries, 5)
        let maxRetryWaitTime =              p.GetResult(CosmosSourceParameters.RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.GetResult(CosmosSourceParameters.Database, fun () -> c.CosmosDatabase)
        let fromTail =                      p.Contains CosmosSourceParameters.FromTail
        let maxItems =                      p.TryGetResult MaxItems
        let lagFrequency: TimeSpan =        p.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        let containerId =                   p.GetResult CosmosSourceParameters.Container
        member val MonitoringParams =       fromTail, maxItems, lagFrequency
        member x.ConnectFeed() =            match x.Archive.MaybeLeasesContainer with
                                            | Some leasesContainerInTarget -> connector.ConnectFeed(database, containerId, leasesContainerInTarget)
                                            | None ->
                                                let leaseContainerId = p.GetResult(CosmosSourceParameters.LeaseContainer, containerId + "-aux")
                                                connector.ConnectFeed(database, containerId, leaseContainerId)
        member val Archive: CosmosSinkArguments =
            match p.GetSubCommand() with
            | DstCosmos cosmos -> CosmosSinkArguments(c, cosmos)
            | _ -> p.Raise "Must specify cosmos for Sink"
    and [<NoEquality; NoComparison>] CosmosSinkParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           $"specify a connection string for a Cosmos account. (optional if environment variable $%s{CONNECTION} specified)"
                | Database _ ->             $"specify a database name for store. (optional if environment variable $%s{DATABASE} specified)"
                | Container _ ->            $"specify a container name for store. (optional if environment variable $%s{CONTAINER} specified)"
                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | Retries _ ->              "specify operation retries. Default: 0."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosSinkArguments(c: Configuration, p: ParseResults<CosmosSinkParameters>) =
        let discovery =                     p.GetResult(Connection, fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let retries =                       p.GetResult(Retries, 0)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, retries, maxRetryWaitTime, ?mode = mode)
        let container =                     p.GetResult(Container, fun () -> c.CosmosContainer)
        let databaseId =                    p.GetResult(Database, fun () -> c.CosmosDatabase)
        let leaseContainerId =              p.TryGetResult LeaseContainer
        member val MaybeLeasesContainer: Microsoft.Azure.Cosmos.Container option = leaseContainerId |> Option.map (fun id -> connector.LeasesContainer(databaseId, id))
        member x.Connect() = async {        // while the default maxJsonBytes is 30000 - we are prepared to incur significant extra write RU charges in order to maximize packing
                                            let maxEvents, maxJsonBytes = 100_000, 100_000
                                            let! context = connector.ConnectContext("Destination", databaseId, container, maxEvents,
                                                                                    tipMaxJsonLength = maxJsonBytes, ?auxContainerId = leaseContainerId)
                                            return Equinox.CosmosStore.Core.EventsContext(context, Store.Metrics.log) }

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv: Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ArchiverTemplate"

let build (args: Args.Arguments) =
    let processorName, maxReadAhead, maxWriters, maxBytes = args.Parameters()
    let log = (Log.forGroup processorName).ForContext<Propulsion.Sinks.Factory>()
    let archiverSink =
        let eventsContext = args.DestinationArchive.Connect() |> Async.RunSynchronously
        let stats = CosmosStoreSinkStats(log, args.StatsInterval, args.StateInterval)
        CosmosStoreSink.Start(log, maxReadAhead, eventsContext, maxWriters, stats,
                              purgeInterval = TimeSpan.FromMinutes 10., maxBytes = maxBytes)
    let monitored, leases = args.Source.ConnectFeed() |> Async.RunSynchronously
    let source =
        let startFromTail, maxItems, lagFrequency = args.Source.MonitoringParams
        CosmosStoreSource(log, args.StatsInterval, monitored, leases, processorName, Handler.selectArchivable, archiverSink,
                          startFromTail = startFromTail, ?maxItems = maxItems, lagEstimationInterval = lagFrequency).Start()
    archiverSink, source

// A typical app will likely have health checks etc, implying the wireup would be via `endpoints.MapMetrics()` and thus not use this ugly code directly
let startMetricsServer port: IDisposable =
    let metricsServer = new Prometheus.KestrelMetricServer(port = port)
    let ms = metricsServer.Start()
    Log.Information("Prometheus /metrics endpoint on port {port}", port)
    { new IDisposable with member x.Dispose() = ms.Stop(); (metricsServer :> IDisposable).Dispose() }

open Propulsion.Internal // AwaitKeyboardInterruptAsTaskCanceledException

let run (args: Args.Arguments) = async {
    let sink, source = build args
    use _metricsServer: IDisposable = args.PrometheusPort |> Option.map startMetricsServer |> Option.toObj
    return! [|  Async.AwaitKeyboardInterruptAsTaskCanceledException()
                source.AwaitWithStopOnCancellation()
                sink.AwaitWithStopOnCancellation()
            |] |> Async.Parallel |> Async.Ignore<unit[]> }

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(AppName, args.Verbose, args.SyncLogging).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? System.Threading.Tasks.TaskCanceledException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? Argu.ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintf $"Exception %s{e.Message}"; 1
