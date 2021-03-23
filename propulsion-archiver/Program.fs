module ArchiverTemplate.Program

open Equinox.Cosmos
open Propulsion.Cosmos
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
        | [<AltCommandLine "-S"; Unique>]   SyncVerbose
        | [<AltCommandLine "-t"; Unique>]   RuThreshold of float
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose

        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 32."
                | MaxWriters _ ->           "maximum number of concurrent writes to target permitted. Default: 4."

                | Verbose ->                "request Verbose Logging. Default: off"
                | SyncVerbose ->            "request Logging for Sync operations (Writes). Default: off"
                | RuThreshold _ ->          "minimum request charge required to log. Default: 0"
                | CfpVerbose ->             "request Verbose Change Feed Processor Logging. Default: off"

                | SrcCosmos _ ->            "Cosmos input parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val ConsumerGroupName =      a.GetResult ConsumerGroupName
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 32)
        member val MaxWriters =             a.GetResult(MaxWriters, 4)
        member val Verbose =                a.Contains Parameters.Verbose
        member val CfpVerbose =             a.Contains CfpVerbose
        member val SyncLogging =            a.Contains SyncVerbose, a.TryGetResult RuThreshold
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val private Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (SrcCosmos cosmos) -> CosmosSourceArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Src")
        member x.SourceParams() =
            let srcC = x.Source
            let disco, db =
                let dstC : CosmosSinkArguments = srcC.Sink
                match srcC.LeaseContainer, dstC.LeaseContainer with
                | None, None ->     srcC.Connection, { database = srcC.Database; container = srcC.Container + "-aux" }
                | Some sc, None ->  srcC.Connection, { database = srcC.Database; container = sc }
                | None, Some dc ->  dstC.Connection, { database = dstC.Database; container = dc }
                | Some _, Some _ -> raise (MissingArg "LeaseContainerSource and LeaseContainerDestination are mutually exclusive - can only store in one database")
            Log.Information("Archiving... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxWriters, x.MaxReadAhead)
            Log.Information("Monitoring Group {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                x.ConsumerGroupName, db.database, db.container, Option.toNullable srcC.MaxDocuments)
            if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            (srcC, (disco, db, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency))
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-md"; Unique>]  MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string

        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
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
                | Container _ ->            "specify a container name within `Database`"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 5."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 30."

                | DstCosmos _ ->            "CosmosDb Sink parameters."
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        member val FromTail =               a.Contains CosmosSourceParameters.FromTail
        member val MaxDocuments =           a.TryGetResult MaxDocuments
        member val LagFrequency =           a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member val LeaseContainer =         a.TryGetResult CosmosSourceParameters.LeaseContainer
        member val Mode =                   a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member val Connection =             a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Discovery.FromConnectionString
        member val Database =               a.TryGetResult CosmosSourceParameters.Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.GetResult CosmosSourceParameters.Container
        member val Timeout =                a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =                a.GetResult(CosmosSourceParameters.Retries, 5)
        member val MaxRetryWaitTime =       a.GetResult(CosmosSourceParameters.RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (DstCosmos cosmos) -> CosmosSinkArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Sink")
        member x.MonitoringParams() =
            let Discovery.UriAndKey (endpointUri, _) as discovery = x.Connection
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let c = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c, discovery, { database = x.Database; container = x.Container }
    and [<NoEquality; NoComparison>] CosmosSinkParameters =
        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
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
        member val Mode =                   a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member val Connection =             a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Discovery.FromConnectionString
        member val Database =               a.TryGetResult Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.TryGetResult Container  |> Option.defaultWith (fun () -> c.CosmosContainer)
        member val LeaseContainer =         a.TryGetResult LeaseContainer
        member val Timeout =                a.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =                a.GetResult(CosmosSinkParameters.Retries, 0)
        member val MaxRetryWaitTime =       a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        /// Connect with the provided parameters and/or environment variables
        member x.Connect appName : Async<Equinox.Cosmos.Connection> =
            let Discovery.UriAndKey (endpointUri, _masterKey) as discovery = x.Connection
            Log.Information("Destination CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Destination CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let c = Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c.Connect(appName, discovery)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ArchiverTemplate"

let build (args : Args.Arguments, log, storeLog : ILogger) =
    let source, (auxDiscovery, aux, leaseId, startFromTail, maxDocuments, lagFrequency) = args.SourceParams()
    let archiverSink =
        let target = source.Sink
        let containers = Containers(target.Database, target.Container)
        let conn = target.Connect AppName |> Async.RunSynchronously
        let context = Equinox.Cosmos.Core.Context(conn, containers, storeLog)
        CosmosSink.Start(log, args.MaxReadAhead, [|context|], args.MaxWriters, args.StatsInterval, args.StateInterval)
    let pipeline =
        let monitoredConnector, monitoredDiscovery, monitored  = source.MonitoringParams()
        let client, auxClient = monitoredConnector.CreateClient(AppName, monitoredDiscovery), monitoredConnector.CreateClient(AppName, auxDiscovery)
        let createObserver () = CosmosSource.CreateObserver(log.ForContext<CosmosSource>(), archiverSink.StartIngester, Seq.collect Handler.selectArchivable)
        CosmosSource.Run(log, client, monitored, aux,
            leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency, auxClient=auxClient)
    archiverSink, pipeline

let run args = async {
    let log, storeLog = Log.ForContext<Propulsion.Streams.Scheduling.StreamSchedulingEngine>(), Log.ForContext<Equinox.Cosmos.Core.Context>()
    let sink, pipeline = build (args, log, storeLog)
    pipeline |> Async.Start
    return! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(args.Verbose, args.SyncLogging, args.CfpVerbose).CreateLogger()
            try run args |> Async.RunSynchronously
                0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
