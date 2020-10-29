﻿module ArchiverTemplate.Program

open Equinox.Cosmos
open Propulsion.Cosmos
open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj
    let set varName value : unit = Environment.SetEnvironmentVariable(varName, value)

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-configuration
// - this is where any custom retrieval of settings not arriving via commandline arguments or environment variables should go
// - values should be propagated by setting environment variables and/or returning them from `initialize`
module Configuration =

    let private initEnvVar var key loadF =
        if None = EnvVar.tryGet var then
            printfn "Setting %s from %A" var key
            EnvVar.set var (loadF key)

    let initialize () =
        // e.g. initEnvVar     "EQUINOX_COSMOS_CONTAINER"    "CONSUL KEY" readFromConsul
        () // TODO add any custom logic preprocessing commandline arguments and/or gathering custom defaults from external sources, etc

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-args
// - this module is responsible solely for parsing/validating the commandline arguments (including falling back to values supplied via environment variables)
// - It's expected that the properties on *Arguments types will summarize the active settings as a side effect of
// TODO DONT invest time reorganizing or reformatting this - half the value is having a legible summary of all program parameters in a consistent value
//      you may want to regenerate it at a different time and/or facilitate comparing it with the `module Args` of other programs
// TODO NEVER hack temporary overrides in here; if you're going to do that, use commandline arguments that fall back to environment variables
//      or (as a last resort) supply them via code in `module Configuration`
module Args =

    exception MissingArg of string
    let private getEnvVarForArgumentOrThrow varName argName =
        match EnvVar.tryGet varName with
        | None -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
        | Some x -> x
    let private defaultWithEnvVar varName argName = function
        | None -> getEnvVarForArgumentOrThrow varName argName
        | Some x -> x

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-g"; Mandatory>] ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int

        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-C"; Unique>]   CfpVerbose

        | [<CliPrefix(CliPrefix.None); AltCommandLine "cosmos"; Unique(*ExactlyOnce is not supported*); Last>] SrcCosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 32."
                | MaxWriters _ ->           "maximum number of concurrent writes to target permitted. Default: 4."

                | Verbose ->                "request Verbose Logging. Default: off"
                | CfpVerbose ->             "request Verbose Change Feed Processor Logging. Default: off"

                | SrcCosmos _ ->            "Cosmos input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead, 32)
        member __.MaxWriters =              a.GetResult(MaxWriters, 4)
        member __.Verbose =                 a.Contains Parameters.Verbose
        member __.CfpVerbose =              a.Contains CfpVerbose
        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member __.StateInterval =           TimeSpan.FromMinutes 5.
        member val private Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (SrcCosmos cosmos) -> (CosmosSourceArguments cosmos)
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")
        member x.SourceParams() =
            let srcC = x.Source
            let disco, db =
                let dstC : CosmosSinkArguments = srcC.Sink
                match srcC.LeaseContainer, dstC.LeaseContainer with
                | None, None ->     srcC.Discovery, { database = srcC.Database; container = srcC.Container + "-aux" }
                | Some sc, None ->  srcC.Discovery, { database = srcC.Database; container = sc }
                | None, Some dc ->  dstC.Discovery, { database = dstC.Database; container = dc }
                | Some _, Some _ -> raise (MissingArg "LeaseContainerSource and LeaseContainerDestination are mutually exclusive - can only store in one database")
            Log.Information("Syncing... Max batches to read ahead: {maxReadAhead} writers: {writers}", x.MaxReadAhead, x.MaxWriters)
            Log.Information("Processing Lease {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
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
    and CosmosSourceArguments(a : ParseResults<CosmosSourceParameters>) =
        member __.FromTail =                a.Contains CosmosSourceParameters.FromTail
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.LeaseContainer =          a.TryGetResult CosmosSourceParameters.LeaseContainer
        member __.Mode =                    a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult CosmosSourceParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosSourceParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.GetResult CosmosSourceParameters.Container
        member __.Timeout =                 a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSourceParameters.Retries, 5)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosSourceParameters.RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (DstCosmos cosmos) -> CosmosSinkArguments cosmos
            | _ -> raise (MissingArg "Must specify cosmos for Sink")
        member x.MonitoringParams() =
            let (Discovery.UriAndKey (endpointUri, _)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let c = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, c
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
    and CosmosSinkArguments(a : ParseResults<CosmosSinkParameters>) =
        member __.Mode =                    a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.TryGetResult Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member __.LeaseContainer =          a.TryGetResult LeaseContainer
        member __.Timeout =                 a.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSinkParameters.Retries, 0)
        member __.MaxRetryWaitTime =        a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        /// Connect with the provided parameters and/or environment variables
        member x.Connect appName : Async<Equinox.Cosmos.Connection> =
            let (Discovery.UriAndKey (endpointUri, _masterKey)) as discovery = x.Discovery
            Log.Information("Destination CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Destination CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let c = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c.Connect(appName, discovery)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        parser.ParseCommandLine argv |> Arguments

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-logging
// Application logic assumes the global `Serilog.Log` is initialized _immediately_ after a successful ArgumentParser.ParseCommandline
module Logging =

    open Serilog.Events
    let initialize verbose changeFeedProcessorVerbose =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
                        let cfpLevel = if changeFeedProcessorVerbose then LogEventLevel.Debug else LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpLevel)
            |> fun c -> let ingesterLevel = if changeFeedProcessorVerbose then LogEventLevel.Debug else LogEventLevel.Information
                        c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamStates<_>>.FullName, ingesterLevel)
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<Propulsion.Cosmos.Internal.Writer.Result>.FullName, generalLevel)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                            a.Logger(fun l ->
                                l.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink()) |> ignore) |> ignore
                            a.Logger(fun l ->
                                let isEqx = Filters.Matching.FromSource<Core.Context>().Invoke
                                let isWriterB = Filters.Matching.FromSource<Propulsion.Cosmos.Internal.Writer.Result>().Invoke
                                let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                                let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                                let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                                let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
                                let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
                                (if changeFeedProcessorVerbose then l else l.Filter.ByExcluding(fun x -> isEqx x || isWriterB x || isCfp x))
                                    .WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                                    |> ignore) |> ignore
                        c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=Action<_> configure)
            |> fun c -> c.CreateLogger()
        Log.ForContext<Propulsion.Streams.Scheduling.StreamStates<_>>(), Log.ForContext<Core.Context>()

let [<Literal>] AppName = "ArchiverTemplate"

let build (args : Args.Arguments, log, storeLog : ILogger) =
    let (source, (auxDiscovery, aux, leaseId, startFromTail, maxDocuments, lagFrequency)) = args.SourceParams()
    let sink =
        let target = source.Sink
        let containers = Containers(target.Database, target.Container)
        let conn = target.Connect AppName |> Async.RunSynchronously
        let context = Equinox.Cosmos.Core.Context(conn, containers, storeLog)
        CosmosSink.Start(log, args.MaxReadAhead, [|context|], args.MaxWriters, args.StatsInterval, args.StateInterval)
    let pipeline =
        let monitoredDiscovery, monitored, monitoredConnector = source.MonitoringParams()
        let client, auxClient = monitoredConnector.CreateClient(AppName, monitoredDiscovery), monitoredConnector.CreateClient(AppName, auxDiscovery)
        let createObserver () = CosmosSource.CreateObserver(log, sink.StartIngester, Seq.collect Handler.transformOrFilter)
        CosmosSource.Run(log, client, monitored, aux,
            leaseId, startFromTail, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency, auxClient=auxClient)
    sink, pipeline

let run (args, log, storeLog) =
    let sink, pipeline = build (args, log, storeLog)
    pipeline |> Async.Start
    sink.AwaitCompletion() |> Async.RunSynchronously
    sink.RanToCompletion

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
        try let log, storeLog = Logging.initialize args.Verbose args.CfpVerbose
            try Configuration.initialize ()
                if run (args, log, storeLog) then 0 else 3
            with e when not (e :? Args.MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
