module TestbedTemplate.Program

open Argu
open Equinox.Cosmos
open FSharp.UMX
open Serilog
open Serilog.Events
open System
open System.Threading
open TestbedTemplate.Infrastructure

[<AutoOpen>]
module CmdParser =
    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    module Cosmos =
        type [<NoEquality; NoComparison>] Arguments =
            | [<AltCommandLine("-vs")>] VerboseStore
            | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
            | [<AltCommandLine("-o")>] Timeout of float
            | [<AltCommandLine("-r")>] Retries of int
            | [<AltCommandLine("-rt")>] RetriesWaitTime of int
            | [<AltCommandLine("-s")>] Connection of string
            | [<AltCommandLine("-d")>] Database of string
            | [<AltCommandLine("-c")>] Collection of string
            | [<AltCommandLine("-a")>] PageSize of int
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | VerboseStore -> "Include low level Store logging."
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                    | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                    | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                    | Database _ ->         "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                    | Collection _ ->       "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
                    | PageSize _ -> "Specify maximum number of events to record on a page before switching to a new one (default: 1)"
        type Info(args : ParseResults<Arguments>) =
            member __.Connection =  match args.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =    match args.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Collection =  match args.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

            member __.Timeout = args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Mode = args.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
            member __.Retries = args.GetResult(Retries, 1)
            member __.MaxRetryWaitTime = args.GetResult(RetriesWaitTime, 5)

    [<NoEquality; NoComparison>]
    type Arguments =
        | [<AltCommandLine("-v")>] Verbose
        | [<AltCommandLine("-vc")>] VerboseConsole
        | [<AltCommandLine("-S")>] LocalSeq
        | [<AltCommandLine("-l")>] LogFile of string
        | [<CliPrefix(CliPrefix.None); Last; Unique>] Run of ParseResults<TestArguments>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Verbose -> "Include low level logging regarding specific test runs."
                | VerboseConsole -> "Include low level test and store actions logging in on-screen output to console."
                | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | LogFile _ -> "specify a log file to write the result breakdown into (default: eqx.log)."
                | Run _ -> "Run a load test"
    and [<NoComparison>]
        TestArguments =
        | [<AltCommandLine("-t"); Unique>] Name of Tests.Test
        | [<AltCommandLine("-s")>] Size of int
        | [<AltCommandLine("-C")>] Cached
        | [<AltCommandLine("-U")>] Unfolds
        | [<AltCommandLine("-m")>] BatchSize of int
        | [<AltCommandLine("-f")>] TestsPerSecond of int
        | [<AltCommandLine("-d")>] DurationM of float
        | [<AltCommandLine("-e")>] ErrorCutoff of int64
        | [<AltCommandLine("-i")>] ReportIntervalS of int
        | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Cosmos.Arguments>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Name _ -> "specify which test to run. (default: Favorite)."
                | Size _ -> "For `-t Todo`: specify random title length max size to use (default 100)."
                | Cached -> "employ a 50MB cache, wire in to Stream configuration."
                | Unfolds -> "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
                | BatchSize _ -> "Maximum item count to supply when querying. Default: 500"
                | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
                | DurationM _ -> "specify a run duration in minutes (default: 30)."
                | ErrorCutoff _ -> "specify an error cutoff; test ends when exceeded (default: 10000)."
                | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
                | Cosmos _ -> "Run transactions in-process against CosmosDb."

module Connection =
    /// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    /// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and collection named "equinox-test"
    /// 2) Set the 3x environment variables and create a local Equinox using tools/Equinox.Tool/bin/Release/net461/eqx.exe `
    ///     init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION
    module Cosmos =
        open CmdParser.Cosmos

        let private createGateway connection (maxItems,maxEvents) = CosmosGateway(connection, CosmosBatchingPolicy(defaultMaxItems=maxItems, maxEventsPerSlice=maxEvents))
        let private ctx (log: ILogger, storeLog: ILogger) (sargs : ParseResults<Arguments>) =
            let read key = Environment.GetEnvironmentVariable key |> Option.ofObj
            let (Discovery.UriAndKey (endpointUri,_)) as discovery =
                sargs.GetResult(Connection, defaultArg (read "EQUINOX_COSMOS_CONNECTION") "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;")
                |> Discovery.FromConnectionString
            let dbName = sargs.GetResult(Database, defaultArg (read "EQUINOX_COSMOS_DATABASE") "equinox-test")
            let collName = sargs.GetResult(Collection, defaultArg (read "EQUINOX_COSMOS_COLLECTION") "equinox-test")
            let timeout = sargs.GetResult(Timeout,5.) |> float |> TimeSpan.FromSeconds
            let mode = sargs.GetResult(ConnectionMode,ConnectionMode.DirectTcp)
            let retries = sargs.GetResult(Retries, 1)
            let maxRetryWaitTime = sargs.GetResult(RetriesWaitTime, 5)
            log.Information("CosmosDb {mode} {connection} Database {database} Collection {collection}", mode, endpointUri, dbName, collName)
            log.Information("CosmosDb timeout: {timeout}s, {retries} retries; Throttling maxRetryWaitTime {maxRetryWaitTime}", timeout.TotalSeconds, retries, maxRetryWaitTime)
            let c = CosmosConnector(log=storeLog, mode=mode, requestTimeout=timeout, maxRetryAttemptsOnThrottledRequests=retries, maxRetryWaitTimeInSeconds=maxRetryWaitTime)
            discovery, dbName, collName, c
        let connectionPolicy (log, storeLog) (sargs : ParseResults<Arguments>) =
            let (Discovery.UriAndKey (endpointUri, masterKey)), dbName, collName, connector = ctx (log, storeLog) sargs
            (endpointUri, masterKey), dbName, collName, connector.ConnectionPolicy
        let connect (log : ILogger, storeLog) (sargs : ParseResults<Arguments>) =
            let discovery, dbName, collName, connector = ctx (log,storeLog) sargs
            let pageSize = sargs.GetResult(PageSize,1)
            log.Information("CosmosDb MaxItems {maxItems} MaxEventsPerSlice {pageSize}", pageSize)
            dbName, collName, pageSize, connector.Connect("equinox-samples", discovery) |> Async.RunSynchronously
        let config (log: ILogger, storeLog) (cache, unfolds, batchSize) (sargs : ParseResults<Arguments>) =
            let dbName, collName, pageSize, conn = connect (log, storeLog) sargs
            let cacheStrategy =
                if cache then
                    let c = Caching.Cache("equinox-tool", sizeMb = 50)
                    CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
                else CachingStrategy.NoCaching
            StorageConfig.Cosmos (createGateway conn (batchSize,pageSize), cacheStrategy, unfolds, dbName, collName)

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Metrics.SerilogHelpers.RuCounterSink())
    let c = c.WriteTo.Console((if verbose && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger

module LoadTest =
    open Microsoft.Extensions.DependencyInjection

    let private runLoadTest log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients.[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let private decorateWithLogger (domainLog : ILogger, verbose) (run: 't -> Async<unit>) =
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        execute
    let private createResultLog fileName = LoggerConfiguration().WriteTo.File(fileName).CreateLogger()
    let run (log: ILogger) (verbose,verboseConsole,maybeSeq) reportFilename (args: Argu.ParseResults<CmdParser.TestArguments>) =
        let storage = args.TryGetSubCommand()

        let createStoreLog verboseStore = createStoreLog verboseStore verboseConsole maybeSeq
        let storeLog, storeConfig: ILogger * StorageConfig =
            let options =  args.GetResults Cached @ args.GetResults Unfolds
            let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds
            match storage with
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Cosmos.Arguments.VerboseStore
                let batchSize = args.GetResult(BatchSize,500)
                log.Information("Running transactions in-process against CosmosDb with storage options: {options:l}", options)
                storeLog, Connection.Cosmos.config (log,storeLog) (cache, unfolds, batchSize) sargs
            | _  -> failwith "must specify cosmos"
        let test =args.GetResult(Name,Tests.Favorite)
        let runSingleTest : ClientId -> Async<unit> =
            let services = ServiceCollection()
            Services.register(services, storeConfig, storeLog)
            let container = services.BuildServiceProvider()
            let execForClient = Tests.executeLocal container test
            decorateWithLogger (log, verbose) execForClient
        let errorCutoff = args.GetResult(ErrorCutoff,10000L)
        let testsPerSecond = args.GetResult(TestsPerSecond,1000)
        let duration = args.GetResult(DurationM,30.) |> TimeSpan.FromMinutes
        let reportingIntervals =
            match args.GetResults(ReportIntervalS) with
            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
            |> fun intervals -> [| yield duration; yield! intervals |]
        let clients = Array.init (testsPerSecond * 2) (fun _ -> % Guid.NewGuid())

        log.Information( "Running {test} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
            test, duration, testsPerSecond, clients.Length, errorCutoff, reportingIntervals, reportFilename)
        let results = runLoadTest log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously

        let resultFile = createResultLog reportFilename
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)

        match storeConfig with
        | (StorageConfig.Cosmos _) ->
            let duration = args.GetResult(DurationM,1.) |> TimeSpan.FromMinutes
            Metrics.dumpStats duration log
        | _ -> ()

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Metrics.SerilogHelpers.RuCounterSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Arguments>(programName = programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = args.Contains Verbose
        let log = createDomainLog verbose verboseConsole maybeSeq
        match args.GetSubCommand() with
        | Run rargs ->
            let reportFilename = args.GetResult(LogFile,programName+".log") |> fun n -> System.IO.FileInfo(n).FullName
            LoadTest.run log (verbose,verboseConsole,maybeSeq) reportFilename rargs
        | _ -> failwith "Please specify a valid subcommand :- init, initAux, project or run"
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1