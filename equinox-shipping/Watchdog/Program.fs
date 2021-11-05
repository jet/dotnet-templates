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
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-g"; Mandatory>] ProcessorName of string
        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   MaxWriters of int
        | [<AltCommandLine "-t"; Unique>]   TimeoutS of float

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                "request Verbose Logging. Default: off."
                | ProcessorName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 8."
                | TimeoutS _ ->             "Timeout (in seconds) before Watchdog should step in to process transactions. Default: 10."
                | Cosmos _ ->               "specify CosmosDB parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        let maxReadAhead =                  a.GetResult(MaxReadAhead, 16)
        let maxConcurrentProcessors =       a.GetResult(MaxWriters, 8)
        member val Verbose =                a.Contains Parameters.Verbose
        member val ProcessorName =          a.GetResult ProcessorName
        member val ProcessManagerMaxDop =   4
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member x.ProcessorParams() =        Log.Information("Watching... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                                                            x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
                                            (x.ProcessorName, maxReadAhead, maxConcurrentProcessors)
        member val ProcessingTimeout =      a.GetResult(TimeoutS, 10.) |> TimeSpan.FromSeconds
        member val Cosmos =                 CosmosArguments(c, a.GetResult Cosmos)
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-mi"; Unique>]  MaxItems of int
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
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "frequency (in minutes) to dump lag stats. Default: 1"
                | LeaseContainer _ ->       "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name within `Database`"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        let discovery =                     a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult ConnectionMode
        let timeout =                       a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(Retries, 1)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   a.GetResult Container
        let leaseContainerId =              a.GetResult(LeaseContainer, containerId + "-aux")
        let fromTail =                      a.Contains FromTail
        let maxItems =                      a.TryGetResult MaxItems
        let lagFrequency =                  a.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member private _.ConnectLeases() =  connector.CreateUninitialized(database, leaseContainerId)
        member x.MonitoringParams() =
            let leases : Microsoft.Azure.Cosmos.Container = x.ConnectLeases()
            Log.Information("ChangeFeed Leases Database {db} Container {container}. MaxItems limited to {maxItems}",
                leases.Database.Id, leases.Id, Option.toNullable maxItems)
            if fromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            (leases, fromTail, maxItems, lagFrequency)
        member _.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(database, containerId)

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

let build (args : Args.Arguments) =
    let processorName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let storeClient, monitored = args.Cosmos.ConnectStoreAndMonitored()
    let storeCfg =
        let context = storeClient |> CosmosStoreContext.create
        let cache = Equinox.Cache (AppName, sizeMb = 10)
        Config.Store.Cosmos (context, cache)
    let sink =
        let processManager = Shipping.Domain.FinalizationWorkflow.Config.create args.ProcessManagerMaxDop storeCfg
        let stats = Handler.Stats(Log.Logger, statsInterval=args.StatsInterval, stateInterval=args.StateInterval)
        startWatchdog Log.Logger (args.ProcessingTimeout, stats) (maxReadAhead, maxConcurrentStreams) processManager.Pump
    let pipeline =
        use observer = CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, Seq.collect Handler.transformOrFilter)
        let leases, startFromTail, maxItems, lagFrequency = args.Cosmos.MonitoringParams()
        CosmosStoreSource.Run(Log.Logger, monitored, leases, processorName, observer, startFromTail, ?maxItems=maxItems, lagReportFreq=lagFrequency)
    sink, pipeline

let run args = async {
    let sink, pipeline = build args
    pipeline |> Async.Start
    return! sink.AwaitWithStopOnCancellation()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
