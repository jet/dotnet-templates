module Shipping.Watchdog.Program

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
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose
        | [<AltCommandLine "-t"; Unique>]   TimeoutS of float

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."
                | Verbose ->                "request Verbose Logging. Default: off."
                | CfpVerbose ->             "request Verbose Change Feed Processor Logging. Default: off."
                | Cosmos _ ->               "specify CosmosDB input parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val ConsumerGroupName =      a.GetResult ConsumerGroupName
        member val Verbose =                a.Contains Verbose
        member val CfpVerbose =             a.Contains CfpVerbose
        member val MaxReadAhead =           a.GetResult(MaxReadAhead, 16)
        member val MaxConcurrentStreams =   a.GetResult(MaxWriters, 8)
        member val ProcessManagerMaxDop =   4
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val ProcessingTimeout =      a.GetResult(TimeoutS, 10.) |> TimeSpan.FromSeconds
        member val Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Cosmos cosmos) -> CosmosSourceArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Src")
        member x.SourceParams() =
            let srcC = x.Source
            let auxColl =
                match srcC.LeaseContainer with
                | None ->     { database = srcC.Database; container = srcC.Container + "-aux" }
                | Some sc ->  { database = srcC.Database; container = sc }
            Log.Information("Watching... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxConcurrentStreams, x.MaxReadAhead)
            Log.Information("Monitoring Group {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
                x.ConsumerGroupName, auxColl.database, auxColl.container, srcC.MaxDocuments)
            if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            srcC.LagFrequency |> Option.iter<TimeSpan> (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds))
            (srcC, (auxColl, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency))
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

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
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
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | Cosmos _ ->               "CosmosDb Sink parameters."
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        member val FromTail =               a.Contains FromTail
        member val MaxDocuments =           a.TryGetResult MaxDocuments
        member val LagFrequency =           a.TryPostProcessResult(LagFreqM, TimeSpan.FromMinutes)
        member val LeaseContainer =         a.TryGetResult LeaseContainer

        member val Mode =                   a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member val Connection =             a.TryGetResult CosmosSourceParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.Cosmos.Discovery.FromConnectionString
        member val Database =               a.TryGetResult CosmosSourceParameters.Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.GetResult CosmosSourceParameters.Container
        member val Timeout =                a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =                a.GetResult(CosmosSourceParameters.Retries, 1)
        member val MaxRetryWaitTime =       a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.MonitoringParams() =
            let Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _) as discovery = x.Connection
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            connector, discovery, { database = x.Database; container = x.Container }
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Cosmos cosmos) -> CosmosArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos details")
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-m">]           ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        member val Mode =                   a.GetResult(CosmosParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member val Connection =             a.TryGetResult CosmosParameters.Connection |> Option.defaultWith (fun () -> c.CosmosConnection)  |> Equinox.Cosmos.Discovery.FromConnectionString
        member val Database =               a.TryGetResult CosmosParameters.Database   |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val Container =              a.TryGetResult CosmosParameters.Container  |> Option.defaultWith (fun () -> c.CosmosContainer)
        member val Timeout =                a.GetResult(CosmosParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =                a.GetResult(CosmosParameters.Retries, 1)
        member val MaxRetryWaitTime =       a.GetResult(CosmosParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _) as discovery = x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            connector, discovery, x.Database, x.Container

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
    let transactions = FinalizationTransaction.Cosmos.create (context, cache)
    let containers = Container.Cosmos.create (context, cache)
    let shipments = Shipment.Cosmos.create (context, cache)
    FinalizationProcessManager.Service(transactions, containers, shipments, maxDop=maxDop)

let build (args : Args.Arguments) =
    let (source, (aux, leaseId, startFromTail, maxDocuments, lagFrequency)) = args.SourceParams()

    // Connect to the Target CosmosDB, wire to Process Manager
    let processManager =
        let connector, discovery, database, container = source.Cosmos.BuildConnectionDetails()
        let connection = connector.Connect(AppName, discovery) |> Async.RunSynchronously
        let cache = Equinox.Cache(AppName, sizeMb=10)
        let context = Equinox.Cosmos.Context(connection, database, container)
        createProcessManager args.ProcessManagerMaxDop (context, cache)

    let sink =
        let stats = Handler.Stats(Serilog.Log.Logger, statsInterval=args.StatsInterval, stateInterval=args.StateInterval)
        startWatchdog Log.Logger (args.ProcessingTimeout, stats) (args.MaxReadAhead, args.MaxConcurrentStreams) processManager.Drive
    let pipeline =
        let createObserver () = CosmosSource.CreateObserver(Log.ForContext<CosmosSource>(), sink.StartIngester, Seq.collect Handler.transformOrFilter)
        let monitoredConnector, monitoredDiscovery, monitored = source.MonitoringParams()
        CosmosSource.Run(Log.Logger, monitoredConnector.CreateClient(AppName, monitoredDiscovery), monitored, aux,
            leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
    sink, pipeline

let run args = async {
    let sink, pipeline = build args
    pipeline |> Async.Start
    return! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose, changeFeedProcessorVerbose=args.CfpVerbose).CreateLogger()
            try run args |> Async.RunSynchronously
                0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
