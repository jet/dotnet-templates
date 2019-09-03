module TestbedTemplate.Program

open Argu
open FSharp.UMX
open Serilog
open Serilog.Events
open System
open System.Threading

[<AutoOpen>]
module CmdParser =
    type [<NoEquality; NoComparison>]
        Parameters =
        | [<AltCommandLine("-v")>] Verbose
        | [<AltCommandLine("-vc")>] VerboseConsole
        | [<AltCommandLine("-S")>] LocalSeq
        | [<AltCommandLine("-l")>] LogFile of string
        | [<CliPrefix(CliPrefix.None); Last; Unique>] Run of ParseResults<TestParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Verbose ->            "Include low level logging regarding specific test runs."
                | VerboseConsole ->     "Include low level test and store actions logging in on-screen output to console."
                | LocalSeq ->           "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | LogFile _ ->          "specify a log file to write the result breakdown into (default: eqx.log)."
                | Run _ ->              "Run a load test"
    and [<NoComparison>]
        TestParameters =
        | [<AltCommandLine("-t"); Unique>] Name of Tests.Test
        | [<AltCommandLine("-s")>] Size of int
        | [<AltCommandLine("-C")>] Cached
        | [<AltCommandLine("-U")>] Unfolds
        | [<AltCommandLine("-m")>] BatchSize of int
        | [<AltCommandLine("-f")>] TestsPerSecond of int
        | [<AltCommandLine("-d")>] DurationM of float
        | [<AltCommandLine("-e")>] ErrorCutoff of int64
        | [<AltCommandLine("-i")>] ReportIntervalS of int
//#if (memoryStore || (!cosmos && !eventStore))
        | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<Storage.MemoryStore.Parameters>
//#endif
//#if eventStore
        | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<Storage.EventStore.Parameters>
//#endif
//#if cosmos
        | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Storage.Cosmos.Parameters>
//#endif
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Name _ ->             "specify which test to run. (default: Favorite)."
                | Size _ ->             "For `-t Todo`: specify random title length max size to use (default 100)."
                | Cached ->             "employ a 50MB cache, wire in to Stream configuration."
                | Unfolds ->            "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
                | BatchSize _ ->        "Maximum item count to supply when querying. Default: 500"
                | TestsPerSecond _ ->   "specify a target number of requests per second (default: 100)."
                | DurationM _ ->        "specify a run duration in minutes (default: 30)."
                | ErrorCutoff _ ->      "specify an error cutoff; test ends when exceeded (default: 10000)."
                | ReportIntervalS _ ->  "specify reporting intervals in seconds (default: 10)."
//#if (memoryStore || (!cosmos && !eventStore))
                | Memory _ ->           "target in-process Transient Memory Store (Default if not other target specified)."
//#endif
//#if eventStore
                | Es _ ->               "Run transactions in-process against EventStore."
//#endif
//#if cosmos
                | Cosmos _ ->           "Run transactions in-process against CosmosDb."
//#endif
    and TestArguments(args: ParseResults<TestParameters>) =
        member __.Options =             args.GetResults Cached @ args.GetResults Unfolds
        member __.Cache =               __.Options |> List.contains Cached
        member __.Unfolds =             __.Options |> List.contains Unfolds
        member __.BatchSize =           args.GetResult(BatchSize,500)
        member __.Test =                args.GetResult(Name,Tests.Favorite)
        member __.ErrorCutoff =         args.GetResult(ErrorCutoff,10000L)
        member __.TestsPerSecond =      args.GetResult(TestsPerSecond,100)
        member __.Duration =            args.GetResult(DurationM,30.) |> TimeSpan.FromMinutes
        member __.ReportingIntervals =
            match args.GetResults(ReportIntervalS) with
            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
            |> fun intervals -> [| yield __.Duration; yield! intervals |]
        member __.ConfigureStore(log : ILogger, createStoreLog) = 
            match args.TryGetSubCommand() with
//#if memoryStore || (!cosmos && !eventStore)
            | Some (Memory _) ->
                log.Warning("Running transactions in-process against Volatile Store with storage options: {options:l}", __.Options)
                createStoreLog false, Storage.MemoryStore.config ()
//#endif
//#if eventStore
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.EventStore.Parameters.VerboseStore
                log.Information("Running transactions in-process against EventStore with storage options: {options:l}", __.Options)
                storeLog, Storage.EventStore.config (log,storeLog) (__.Cache, __.Unfolds, __.BatchSize) sargs
//#endif
//#if cosmos
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.Cosmos.Parameters.VerboseStore
                log.Information("Running transactions in-process against CosmosDb with storage options: {options:l}", __.Options)
                storeLog, Storage.Cosmos.config (log,storeLog) (__.Cache, __.Unfolds, __.BatchSize) (Storage.Cosmos.Arguments sargs)
//#endif
#if ((!cosmos && !eventStore) || (cosmos && eventStore))
            | _ -> raise <| Storage.MissingArg (sprintf "Please identify a valid store: memory, es, cosmos")
#endif
#if eventStore
            | _ -> raise <| Storage.MissingArg (sprintf "Please identify a valid store: memory, es")
#endif
#if cosmos
            | _ -> raise <| Storage.MissingArg (sprintf "Please identify a valid store: memory, cosmos")
#endif

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
//#if eventStore
    let c = c.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
//#endif
//#if cosmos
    let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
//#endif
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
        Equinox.Tools.TestHarness.Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let private decorateWithLogger (domainLog : ILogger, verbose) (run: 't -> Async<unit>) =
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        execute
    let private createResultLog fileName = LoggerConfiguration().WriteTo.File(fileName).CreateLogger()
    let run (log: ILogger) (verbose,verboseConsole,maybeSeq) reportFilename (a : CmdParser.TestArguments) =
        let createStoreLog verboseStore = createStoreLog verboseStore verboseConsole maybeSeq
        let storeLog, storeConfig: ILogger * Storage.StorageConfig = a.ConfigureStore(log, createStoreLog)
        let runSingleTest : ClientId -> Async<unit> =
            let services = ServiceCollection()
            Services.register(services, storeConfig, storeLog)
            let container = services.BuildServiceProvider()
            let execForClient = Tests.executeLocal container a.Test
            decorateWithLogger (log, verbose) execForClient
        let clients = Array.init (a.TestsPerSecond * 2) (fun _ -> % Guid.NewGuid())
        let duration, intervals = a.Duration, a.ReportingIntervals
        log.Information( "Running {test} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
            a.Test, a.Duration, a.TestsPerSecond, clients.Length, a.ErrorCutoff, intervals, reportFilename)
        let results = runLoadTest log a.TestsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) a.ErrorCutoff intervals clients runSingleTest |> Async.RunSynchronously

        let resultFile = createResultLog reportFilename
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)
//#if cosmos || eventStore

        match storeConfig with
//#if cosmos
        | Storage.StorageConfig.Cosmos _ ->
            Equinox.Cosmos.Store.Log.InternalMetrics.dump log
//#endif
//#if eventStore
        | Storage.StorageConfig.Es _ ->
            Equinox.EventStore.Log.InternalMetrics.dump log
//#endif
//#if memory
        | _ -> ()
//#endif
//#endif

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
//#if eventStore
    let c = c.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
//#endif
//#if cosmos
    let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
//#endif
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Parameters>(programName = programName)
    try
        let args = parser.ParseCommandLine argv
        match args.GetSubCommand() with
        | Run rargs ->
            let verboseConsole = args.Contains VerboseConsole
            let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
            let verbose = args.Contains Verbose
            let log = createDomainLog verbose verboseConsole maybeSeq
            let reportFilename = args.GetResult(LogFile,programName+".log") |> fun n -> System.IO.FileInfo(n).FullName
            LoadTest.run log (verbose,verboseConsole,maybeSeq) reportFilename (TestArguments rargs)
        | _ -> failwith "Please specify a valid subcommand :- run"
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | Storage.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1