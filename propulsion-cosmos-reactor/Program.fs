module ReactorTemplate.Program

open Serilog
open System

exception MissingArg of message : string with override this.Message = this.message

type Configuration(tryGet) =

    let get key =
        match tryGet key with
        | Some value -> value
        | None -> raise (MissingArg (sprintf "Missing Argument/Environment Variable %s" key))

    member _.CosmosConnection =                 get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =                   get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =                  get "EQUINOX_COSMOS_CONTAINER"

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]       Verbose
        | [<AltCommandLine "-g"; Mandatory>]    ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]       MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]       MaxWriters of int
        | [<CliPrefix(CliPrefix.None); Last>]   Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                    "request Verbose Logging. Default: off."
                | ConsumerGroupName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->             "maximum number of batches to let processing get ahead of completion. Default: 2."
                | MaxWriters _ ->               "maximum number of concurrent streams on which to process at any time. Default: 8."
                | Cosmos _ ->                   "specify CosmosDB input parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        let maxReadAhead =                      a.GetResult(MaxReadAhead, 2)
        let maxConcurrentProcessors =           a.GetResult(MaxWriters, 8)
        member val Verbose =                    a.Contains Verbose
        member val ConsumerGroupName =          a.GetResult ConsumerGroupName
        member x.ProcessorParams() =
            Log.Information("Projecting... {processorName}, reading {maxReadAhead} ahead, {dop} writers",
                            x.ConsumerGroupName, maxReadAhead, maxConcurrentProcessors)
            (x.ConsumerGroupName, maxReadAhead, maxConcurrentProcessors)
        member val StatsInterval =              TimeSpan.FromMinutes 1.
        member val StateInterval =              TimeSpan.FromMinutes 2.
        member val Cosmos =                     CosmosArguments (c, a.GetResult Cosmos)
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-m">]               ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]               Connection of string
        | [<AltCommandLine "-d">]               Database of string
        | [<AltCommandLine "-c">]               Container of string
        | [<AltCommandLine "-o">]               Timeout of float
        | [<AltCommandLine "-r">]               Retries of int
        | [<AltCommandLine "-rt">]              RetriesWaitTime of float

        | [<AltCommandLine "-C"; Unique>]       CfpVerbose
        | [<AltCommandLine "-a"; Unique>]       LeaseContainer of string
        | [<AltCommandLine "-Z"; Unique>]       FromTail
        | [<AltCommandLine "-md"; Unique>]      MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]       LagFreqM of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->           "override the connection mode. Default: Direct."
                | Connection _ ->               "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->                 "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->                "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->                  "specify operation timeout in seconds. Default: 5."
                | Retries _ ->                  "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->          "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | CfpVerbose ->                 "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | LeaseContainer _ ->           "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | FromTail _ ->                 "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->             "maximum document count to supply for the Change Feed query. Default: use response size limit"
                | LagFreqM _ ->                 "specify frequency (minutes) to dump lag stats. Default: 1"
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        let discovery =                         a.TryGetResult CosmosParameters.Connection
                                                |> Option.defaultWith (fun () -> c.CosmosConnection)
                                                |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                              a.TryGetResult ConnectionMode
        let timeout =                           a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                           a.GetResult(Retries, 1)
        let maxRetryWaitTime =                  a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                         Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                          a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                       a.GetResult Container

        let leaseContainerId =                  a.GetResult(LeaseContainer, containerId + "-aux")
        let fromTail =                          a.Contains FromTail
        let maxDocuments =                      a.TryGetResult MaxDocuments
        let lagFrequency =                      a.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member _.CfpVerbose =                   a.Contains CfpVerbose
        member private _.ConnectLeases() =      connector.CreateUninitialized(database, leaseContainerId)
        member x.MonitoringParams() =
            let leases : Microsoft.Azure.Cosmos.Container = x.ConnectLeases()
            Log.Information("Monitoring Database {database} Container {container} with maximum document count limited to {maxDocuments}",
                leases.Database.Id, leases.Id, Option.toNullable maxDocuments)
            if fromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            let lagFreq = lagFrequency in Log.Information("ChangeFeed Lag stats interval {lagS:n0}s", lagFreq.TotalSeconds)
            (leases, fromTail, maxDocuments, lagFreq)
        member x.ConnectStoreAndMonitored() =   connector.ConnectStoreAndMonitored(database, containerId)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ReactorTemplate"

let build (args : Args.Arguments) =
    let consumerGroupName, maxReadAhead, maxConcurrentStreams = args.ProcessorParams()
    let client, monitored = args.Cosmos.ConnectStoreAndMonitored()
    let sink =
        let handle =
            let context = client |> CosmosStoreContext.create
            let cache = Equinox.Cache(AppName, sizeMb=10)
            let srcService = Todo.Cosmos.create (context, cache)
            let dstService = TodoSummary.Cosmos.create (context, cache)
            Reactor.handle srcService dstService
        let stats = Reactor.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
        Propulsion.Streams.StreamsProjector.Start(Log.Logger, maxReadAhead, maxConcurrentStreams, handle, stats, args.StatsInterval)

    let pipeline =
        let parseFeedDoc : _ -> Propulsion.Streams.StreamEvent<_> seq = Seq.collect Propulsion.CosmosStore.EquinoxNewtonsoftParser.enumStreamEvents
        use observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, parseFeedDoc)
        let leases, startFromTail, maxDocuments, lagFrequency = args.Cosmos.MonitoringParams()
        Propulsion.CosmosStore.CosmosStoreSource.Run(Log.Logger, monitored, leases, consumerGroupName, observer, startFromTail, ?maxDocuments=maxDocuments, lagReportFreq=lagFrequency)
    sink, pipeline

let run args = async {
    let sink, pipeline = build args
    pipeline |> Async.Start
    return! sink.AwaitCompletion()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(args.Verbose).AsChangeFeedProcessor(AppName, args.ConsumerGroupName, args.Cosmos.CfpVerbose).Default()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintfn "Exception %s" e.Message; 1
