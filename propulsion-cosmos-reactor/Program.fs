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
        | [<AltCommandLine "-g"; Mandatory>]    ConsumerGroupName of string
        | [<AltCommandLine "-r"; Unique>]       MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]       MaxWriters of int
        | [<AltCommandLine "-V"; Unique>]       Verbose
        | [<AltCommandLine "-C"; Unique>]       CfpVerbose
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSourceParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConsumerGroupName _ ->        "Projector consumer group name."
                | MaxReadAhead _ ->             "maximum number of batches to let processing get ahead of completion. Default: 16."
                | MaxWriters _ ->               "maximum number of concurrent streams on which to process at any time. Default: 8."
                | Verbose ->                    "request Verbose Logging. Default: off."
                | CfpVerbose ->                 "request Verbose Change Feed Processor Logging. Default: off."
                | Cosmos _ ->                   "specify CosmosDB input parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val ConsumerGroupName =          a.GetResult ConsumerGroupName
        member val CfpVerbose =                 a.Contains CfpVerbose
        member val MaxReadAhead =               a.GetResult(MaxReadAhead, 16)
        member val MaxConcurrentStreams =       a.GetResult(MaxWriters, 8)
        member val Verbose =                    a.Contains Parameters.Verbose
        member val StatsInterval =              TimeSpan.FromMinutes 1.
        member val StateInterval =              TimeSpan.FromMinutes 5.
        member val Source : CosmosSourceArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Cosmos cosmos) -> CosmosSourceArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos for Src")
        member x.SourceParams() =
            let srcC = x.Source
            let leases = srcC.ConnectLeases()
            Log.Information("Reacting... {dop} writers, max {maxReadAhead} batches read ahead", x.MaxConcurrentStreams, x.MaxReadAhead)
            Log.Information("Monitoring Group {processorName} in Database {database} Container {container} with maximum document count limited to {maxDocuments}",
                x.ConsumerGroupName, srcC.DatabaseId, srcC.ContainerId, Option.toNullable srcC.MaxDocuments)
            if srcC.FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            srcC.LagFrequency |> Option.iter<TimeSpan> (fun i -> Log.Information("ChangeFeed Lag stats interval {lagS:n0}s", i.TotalSeconds))
            let storeClient, monitored = srcC.ConnectStoreAndMonitored()
            let context = CosmosStoreContext.create storeClient
            (srcC, context, monitored, leases, x.ConsumerGroupName, srcC.FromTail, srcC.MaxDocuments, srcC.LagFrequency)
    and [<NoEquality; NoComparison>] CosmosSourceParameters =
        | [<AltCommandLine "-Z"; Unique>]       FromTail
        | [<AltCommandLine "-md"; Unique>]      MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>]       LagFreqM of float
        | [<AltCommandLine "-a"; Unique>]       LeaseContainer of string

        | [<AltCommandLine "-m">]               ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]               Connection of string
        | [<AltCommandLine "-d">]               Database of string
        | [<AltCommandLine "-c"; Unique>]       Container of string // Actually Mandatory, but stating that is not supported
        | [<AltCommandLine "-o">]               Timeout of float
        | [<AltCommandLine "-r">]               Retries of int
        | [<AltCommandLine "-rt">]              RetriesWaitTime of float

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | FromTail ->                   "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->             "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->                 "frequency (in minutes) to dump lag stats. Default: off"
                | LeaseContainer _ ->           "specify Container Name for Leases container. Default: `sourceContainer` + `-aux`."

                | ConnectionMode _ ->           "override the connection mode. Default: Direct."
                | Connection _ ->               "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->                 "specify a database name for Cosmos account. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->                "specify a container name within `Database`"
                | Timeout _ ->                  "specify operation timeout in seconds. Default: 5."
                | Retries _ ->                  "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->          "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."

                | Cosmos _ ->                   "CosmosDb Sink parameters."
    and CosmosSourceArguments(c : Configuration, a : ParseResults<CosmosSourceParameters>) =
        let discovery =                         a.TryGetResult CosmosSourceParameters.Connection
                                                |> Option.defaultWith (fun () -> c.CosmosConnection)
                                                |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                              a.TryGetResult CosmosSourceParameters.ConnectionMode
        let timeout =                           a.GetResult(CosmosSourceParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                           a.GetResult(CosmosSourceParameters.Retries, 1)
        let maxRetryWaitTime =                  a.GetResult(CosmosSourceParameters.RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                         Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        member val DatabaseId =                 a.TryGetResult CosmosSourceParameters.Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val ContainerId =                a.GetResult CosmosSourceParameters.Container

        member val FromTail =                   a.Contains CosmosSourceParameters.FromTail
        member val MaxDocuments =               a.TryGetResult MaxDocuments
        member val LagFrequency =               a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member val private LeaseContainerId =   a.TryGetResult CosmosSourceParameters.LeaseContainer
        member private x.ConnectLeases containerId = connector.CreateUninitialized(x.DatabaseId, containerId)
        member x.ConnectLeases() =              match x.LeaseContainerId with
                                                | None ->    x.ConnectLeases(x.ContainerId + "-aux")
                                                | Some sc -> x.ConnectLeases(sc)
        member x.ConnectStoreAndMonitored() =   connector.ConnectStoreAndMonitored(x.DatabaseId, x.ContainerId)
        member val Cosmos =
            match a.TryGetSubCommand() with
            | Some (CosmosSourceParameters.Cosmos cosmos) -> CosmosArguments (c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos details")
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]               Connection of string
        | [<AltCommandLine "-m">]               ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-d">]               Database of string
        | [<AltCommandLine "-c">]               Container of string
        | [<AltCommandLine "-o">]               Timeout of float
        | [<AltCommandLine "-r">]               Retries of int
        | [<AltCommandLine "-rt">]              RetriesWaitTime of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->           "override the connection mode. Default: Direct."
                | Connection _ ->               "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->                 "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->                "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->                  "specify operation timeout in seconds. Default: 5."
                | Retries _ ->                  "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->          "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        let discovery =                         a.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                              a.TryGetResult ConnectionMode
        let timeout =                           a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                           a.GetResult(Retries, 1)
        let maxRetryWaitTime =                  a.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                         Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode=mode)
        member val DatabaseId =                 a.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        member val ContainerId =                a.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        member x.Connect() =                    connector.ConnectStore("Main", x.DatabaseId, x.ContainerId)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ReactorTemplate"

let build (args : Args.Arguments) =
    let source, context, monitored, leases, processorName, startFromTail, maxDocuments, lagFrequency = args.SourceParams()

    let context = source.Cosmos.Connect() |> Async.RunSynchronously |> CosmosStoreContext.create
    let cache = Equinox.Cache(AppName, sizeMb=10)
    let srcService = Todo.Cosmos.create (context, cache)
    let dstService = TodoSummary.Cosmos.create (context, cache)
    let handle = Reactor.handle srcService dstService
    let stats = Reactor.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
    let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.MaxConcurrentStreams, handle, stats, args.StatsInterval)

    let mapToStreamItems docs : Propulsion.Streams.StreamEvent<_> seq =
        docs
        |> Seq.collect Propulsion.CosmosStore.EquinoxNewtonsoftParser.enumStreamEvents
    let pipeline =
        use observer = Propulsion.CosmosStore.CosmosStoreSource.CreateObserver(Log.Logger, sink.StartIngester, mapToStreamItems)
        Propulsion.CosmosStore.CosmosStoreSource.Run(Log.Logger, monitored, leases, processorName, observer, startFromTail, ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency)
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
