module FeedConsumerTemplate.Program

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

    member _.BaseUri =                      get "API_BASE_URI"
    member _.Group =                        get "API_CONSUMER_GROUP"

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose

        | [<AltCommandLine "-g"; Unique>]   Group of string
        | [<AltCommandLine "-f"; Unique>]   BaseUri of string

        | [<AltCommandLine "-r"; Unique>]   MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>]   FcsDop of int
        | [<AltCommandLine "-t"; Unique>]   TicketsDop of int

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose _ ->              "request verbose logging."
                | Group _ ->                "specify Api Consumer Group Id. (optional if environment variable API_CONSUMER_GROUP specified)"
                | BaseUri _ ->              "specify Api endpoint. (optional if environment variable API_BASE_URI specified)"
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 8."
                | FcsDop _ ->               "maximum number of FCs to process in parallel. Default: 4"
                | TicketsDop _ ->           "maximum number of Tickets to process in parallel (per FC). Default: 4"
                | Cosmos _ ->               "Cosmos Store parameters."
    and Arguments(c : Configuration, a : ParseResults<Parameters>) =
        member val Verbose =                a.Contains Parameters.Verbose
        member val Group =                  a.TryGetResult Group        |> Option.defaultWith (fun () -> c.Group)
        member val BaseUri =                a.TryGetResult BaseUri      |> Option.defaultWith (fun () -> c.BaseUri) |> Uri
        member val MaxReadAhead =           a.GetResult(MaxReadAhead,8)
        member val FcsDop =                 a.TryGetResult FcsDop       |> Option.defaultValue 4
        member val TicketsDop =             a.TryGetResult TicketsDop   |> Option.defaultValue 4
        member val StatsInterval =          TimeSpan.FromMinutes 1.
        member val StateInterval =          TimeSpan.FromMinutes 5.
        member val CheckpointInterval =     TimeSpan.FromHours 1.
        member val Cosmos : CosmosArguments =
            match a.TryGetSubCommand() with
            | Some (Cosmos cosmos) -> CosmosArguments(c, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos")
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-cm">]          ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds (default: 30)."
                | Retries _ ->              "specify operation retries (default: 9)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 30)"
    and CosmosArguments(c : Configuration, a : ParseResults<CosmosParameters>) =
        let discovery =                     a.TryGetResult Connection   |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          a.TryGetResult ConnectionMode
        let timeout =                       a.GetResult(Timeout, 30.)   |> TimeSpan.FromSeconds
        let retries =                       a.GetResult(Retries, 9)
        let maxRetryWaitTime =              a.GetResult(RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode=mode)
        let database =                      a.TryGetResult Database     |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let container =                     a.TryGetResult Container    |> Option.defaultWith (fun () -> c.CosmosContainer)
        member _.Connect() =                connector.ConnectStore("Main", database, container)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "FeedConsumerTemplate"

let build (args : Args.Arguments) =
    let cache = Equinox.Cache (AppName, sizeMb = 10)
    let context = args.Cosmos.Connect() |> Async.RunSynchronously |> CosmosStoreContext.create

    let sink =
        let handle = Ingester.handle args.TicketsDop
        let stats = Ingester.Stats(Log.Logger, args.StatsInterval, args.StateInterval)
        Propulsion.Streams.StreamsProjector.Start(Log.Logger, args.MaxReadAhead, args.FcsDop, handle, stats, args.StatsInterval)
    let pumpSource =
        let sourceId, tailSleepInterval = Propulsion.Feed.SourceId.parse args.Group, TimeSpan.FromSeconds 1.
        let checkpoints = Propulsion.Feed.ReaderCheckpoint.CosmosStore.create Equinox.log (context, cache)
        let feed = ApiClient.TicketsFeed args.BaseUri
        let source =
            Propulsion.Feed.FeedSource(
                Log.Logger, args.StatsInterval, sourceId, tailSleepInterval,
                checkpoints, args.CheckpointInterval, feed.Poll, sink)
        source.Pump feed.ReadTranches
    sink, pumpSource

[<AutoOpen>]
module ConsumerExt =
    type Propulsion.Pipeline with
        member sink.AwaitWithStopOnCancellation() = async {
            let! ct = Async.CancellationToken
            use _ = ct.Register(fun () -> sink.Stop())
            return! sink.AwaitWithStopOnCancellation()
        }

let run args = async {
    let sink, pumpSource = build args
    do! Async.Parallel [ pumpSource; sink.AwaitWithStopOnCancellation() ] |> Async.Ignore<unit[]>
    return if sink.RanToCompletion then 0 else 3
}

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                    c.WriteTo.Console(theme=theme, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {NewLine}{Exception}")

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
