module Watchdog.Program

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
        // e.g. initEnvVar     "EQUINOX_COSMOS_COLLECTION"    "CONSUL KEY" readFromConsul
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
        | [<AltCommandLine "-C"; Unique>]   VerboseConsole

        | [<AltCommandLine "-e">]           CategoryBlacklist of string
        | [<AltCommandLine "-i">]           CategoryWhitelist of string
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | Verbose ->                "request Verbose Logging. Default: off."
                | VerboseConsole ->         "request Verbose Console Logging. Default: off."
                | CategoryBlacklist _ ->    "category whitelist"
                | CategoryWhitelist _ ->    "category blacklist"
                | Cosmos _ ->               "specify CosmosDB input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.ConsumerGroupName =       a.GetResult ConsumerGroupName
        member __.Verbose =                 a.Contains Parameters.Verbose
        member __.VerboseConsole =          a.Contains VerboseConsole
        member __.MaxReadAhead =            a.GetResult(MaxReadAhead, 16)
        member __.MaxConcurrentStreams =    a.GetResult(MaxWriters, 8)
        member __.StatsInterval =           TimeSpan.FromMinutes 1.
        member __.FilterFunction(?excludeLong, ?longOnly): string -> bool =
            let isLong (streamName : string) =
                streamName.StartsWith "Inventory-" // Too long
                || streamName.StartsWith "InventoryCount-" // No Longer used
                || streamName.StartsWith "InventoryLog" // 5GB, causes lopsided partitions, unused
            let excludeLong = defaultArg excludeLong true
            match a.GetResults CategoryBlacklist, a.GetResults CategoryWhitelist with
            | [], [] when longOnly = Some true ->
                Log.Information("Only including long streams")
                isLong
            | [], [] ->
                let black = set [
                    "SkuFileUpload-534e4362c641461ca27e3d23547f0852"
                    "SkuFileUpload-778f1efeab214f5bab2860d1f802ef24"
                    "PurchaseOrder-5791" ]
                let isCheckpoint (streamName : string) =
                    streamName.EndsWith "_checkpoint"
                    || streamName.EndsWith "_checkpoints"
                    || streamName.StartsWith "#serial"
                    || streamName.StartsWith "marvel_bookmark"
                Log.Information("Using well-known stream blacklist {black} excluding checkpoints and #serial streams, excluding long streams: {excludeLong}", black, excludeLong)
                fun x -> not (black.Contains x) && (not << isCheckpoint) x && (not excludeLong || (not << isLong) x)
            | bad, [] ->    let black = Set.ofList bad in Log.Warning("Excluding categories: {cats}", black); fun x -> not (black.Contains x)
            | [], good ->   let white = Set.ofList good in Log.Warning("Only copying categories: {cats}", white); fun x -> white.Contains x
            | _, _ -> raise (MissingArg "BlackList and Whitelist are mutually exclusive; inclusions and exclusions cannot be mixed")
        member val Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Cosmos cosmos) -> CosmosSourceArguments cosmos
            | _ -> raise (MissingArg "Must specify cosmos for Src")
        member x.SourceParams() =
                let srcC = x.Source
                let auxColl =
                    match srcC.LeaseContainer with
                    | None ->     { database = srcC.Database; container = srcC.Container + "-aux" }
                    | Some sc ->  { database = srcC.Database; container = sc }
                Log.Information("Max read backlog: {maxReadAhead}", x.MaxReadAhead)
                Log.Information("Processing Lease {leaseId} in Database {db} Container {container} with maximum document count limited to {maxDocuments}",
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
    and CosmosSourceArguments(a : ParseResults<CosmosSourceParameters>) =
        member __.FromTail =                a.Contains CosmosSourceParameters.FromTail
        member __.MaxDocuments =            a.TryGetResult MaxDocuments
        member __.LagFrequency =            a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.LeaseContainer =          a.TryGetResult CosmosSourceParameters.LeaseContainer

        member __.Mode =                    a.GetResult(CosmosSourceParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Discovery =               Equinox.Cosmos.Discovery.FromConnectionString __.Connection
        member __.Connection =              a.TryGetResult CosmosSourceParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosSourceParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.GetResult CosmosSourceParameters.Container
        member __.Timeout =                 a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosSourceParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let (Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; container = x.Container }, connector
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Cosmos cosmos) -> CosmosArguments cosmos
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
    and CosmosArguments(a : ParseResults<CosmosParameters>) =
        member __.Mode =                    a.GetResult(CosmosParameters.ConnectionMode, Equinox.Cosmos.ConnectionMode.Direct)
        member __.Connection =              a.TryGetResult CosmosParameters.Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =                a.TryGetResult CosmosParameters.Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =               a.TryGetResult CosmosParameters.Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member __.Timeout =                 a.GetResult(CosmosParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =                 a.GetResult(CosmosParameters.Retries, 1)
        member __.MaxRetryWaitTime =        a.GetResult(CosmosParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        member x.BuildConnectionDetails() =
            let (Equinox.Cosmos.Discovery.UriAndKey (endpointUri, _) as discovery) = Equinox.Cosmos.Discovery.FromConnectionString x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Container {container}",
                x.Mode, endpointUri, x.Database, x.Container)
            Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            let connector = Equinox.Cosmos.Connector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, x.Database, x.Container, connector

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

// TODO remove this entire comment after reading https://github.com/jet/dotnet-templates#module-logging
// Application logic assumes the global `Serilog.Log` is initialized _immediately_ after a successful Args.parse
module Logging =

    let initialize verbose verboseConsole =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            // LibLog writes to the global logger, so we need to control the emission
            |> fun c -> let cfpl = if verboseConsole then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
                        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
                        if verboseConsole then c else c.Filter.ByExcluding(fun x -> isCfp x)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()

let [<Literal>] AppName = "Watchdog"

let build (args : Args.Arguments) =
        let (source, (aux, leaseId, startFromTail, maxDocuments, lagFrequency)) = args.SourceParams()
        let monitoredDiscovery, monitored, monitoredConnector = source.BuildConnectionDetails()
        let cosmos = source.Cosmos
        let (discovery, database, container, connector) = cosmos.BuildConnectionDetails()

        // TODO: establish any relevant inputs, or re-run without `-blank` for example wiring code
        let handle = Ingester.handleStreamEvents Ingester.tryHandle
        let stats = Ingester.Stats(Log.Logger, statsInterval = TimeSpan.FromMinutes 1., stateInterval = TimeSpan.FromMinutes 5.)
        let filterByStreamName = args.FilterFunction()
        let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats = stats)

        let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamEvent<_> seq =
            // TODO: customize parsing to events if source is not an Equinox Container
            docs
            |> Seq.collect EquinoxCosmosParser.enumStreamEvents
            |> Seq.filter (fun e -> e.stream |> FsCodec.StreamName.toString |> filterByStreamName)
        let createObserver () = CosmosSource.CreateObserver(Log.Logger, sink.StartIngester, mapToStreamItems)
        let runPipeline =
            CosmosSource.Run(Log.Logger, monitoredConnector.CreateClient(AppName, monitoredDiscovery), monitored, aux,
                leaseId, startFromTail, createObserver,
                ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
        sink, runPipeline

let run args =
    let projector, runSourcePipeline = build args
    runSourcePipeline |> Async.Start
    projector.AwaitCompletion() |> Async.RunSynchronously
    projector.RanToCompletion

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
        try Logging.initialize args.Verbose args.VerboseConsole
            try Configuration.initialize ()
                if run args then 0 else 3
            with e -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
