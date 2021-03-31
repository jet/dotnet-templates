module FeedApiTemplate.Program

open Serilog
open System

exception MissingArg of message : string with override this.Message = this.message

type Configuration(tryGet) =

    let get key =
        match tryGet key with
        | Some value -> value
        | None -> raise (MissingArg (sprintf "Missing Argument/Environment Variable %s" key))

    member _.EquinoxCosmosConnection        = get "EQUINOX_COSMOS_CONNECTION"
    member _.EquinoxCosmosDatabase          = get "EQUINOX_COSMOS_DATABASE"
    member _.EquinoxCosmosContainer         = get "EQUINOX_COSMOS_CONTAINER"

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Verbose ->                "request Verbose Logging. Default: off."
                | Cosmos _ ->               "specify CosmosDB input parameters."
    and Arguments(config : Configuration, a : ParseResults<Parameters>) =
        member val Verbose =                a.Contains Parameters.Verbose
        member val Cosmos : CosmosArguments =
            match a.TryGetSubCommand() with
            | Some (Parameters.Cosmos cosmos) -> CosmosArguments(config, cosmos)
            | _ -> raise (MissingArg "Must specify cosmos")
    and [<NoEquality; NoComparison>] CosmosParameters =
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
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
    and CosmosArguments(config : Configuration, a : ParseResults<CosmosParameters>) =
        member val Mode =                    a.GetResult(CosmosParameters.ConnectionMode, Microsoft.Azure.Cosmos.ConnectionMode.Direct)
        member val Connection =              a.TryGetResult CosmosParameters.Connection |> Option.defaultWith (fun () -> config.EquinoxCosmosConnection)
        member val Timeout =                 a.GetResult(CosmosParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member val Retries =                 a.GetResult(CosmosParameters.Retries, 9)
        member val MaxRetryWaitTime =        a.GetResult(CosmosParameters.RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        member x.CreateClient() =
            let discovery = Equinox.CosmosStore.Discovery.ConnectionString x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                x.Mode, discovery.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries, (let t = x.MaxRetryWaitTime in t.TotalSeconds))
            Equinox.CosmosStore.CosmosStoreClientFactory(x.Timeout, x.Retries, x.MaxRetryWaitTime, mode=x.Mode)
                //.Connect(discovery)
                .CreateUninitialized(discovery)

        member val Database =                a.TryGetResult CosmosParameters.Database  |> Option.defaultWith (fun () -> config.EquinoxCosmosDatabase)
        member val Container =               a.TryGetResult CosmosParameters.Container |> Option.defaultWith (fun () -> config.EquinoxCosmosContainer)
//        member x.Connect(connector) =
        member x.Connect(client) =
            Log.Information("CosmosDb Database {database} Container {container}", x.Database, x.Container)
            Equinox.CosmosStore.CosmosStoreConnection(client, x.Database, x.Container)

    /// Parse the commandline; can throw MissingArg or Argu.ArguParseException in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName=programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "Financials.Api"

open Microsoft.Extensions.DependencyInjection

let registerSingleton<'t when 't : not struct> (services : IServiceCollection) (s : 't) =
    services.AddSingleton s |> ignore

[<System.Runtime.CompilerServices.Extension>]
type AppDependenciesExtensions() =

    [<System.Runtime.CompilerServices.Extension>]
    static member AddTickets(services : IServiceCollection, context, cache) : unit = Async.RunSynchronously <| async {

        let ticketsSeries = Domain.TicketsSeries.Cosmos.create (context, cache)
        let ticketsEpochs = Domain.TicketsEpoch.Cosmos.createReader (context, cache)
        let tickets = Domain.Tickets.Cosmos.create (context, cache)

        ticketsSeries |> registerSingleton services
        ticketsEpochs |> registerSingleton services
        tickets |> registerSingleton services
    }

open Microsoft.Extensions.Hosting

[<EntryPoint>]
let main argv =
    try Log.Logger <- LoggerConfiguration().Configure().CreateLogger()
        try let args = Args.parse EnvVar.tryGet argv

            let cosmos = args.Cosmos
        //    let connector = cosmos.CreateConnector()
        //    let! storeClient = cosmos.Connect(connector)
        //    let conn = Equinox.Cosmos.Connection(storeClient)
        //    let context = Equinox.Cosmos.Context
            let context = cosmos.CreateClient() |> cosmos.Connect |> Equinox.CosmosStore.CosmosStoreContext
            let cache = Equinox.Cache(AppName, sizeMb=2)

            Hosting.createHostBuilder()
                .ConfigureServices(fun s ->
                    s.AddTickets(context, cache))
                .Build()
                .Run()
            0
        with
        | :? Argu.ArguParseException
        | :? MissingArg as e ->
            eprintfn "%s" e.Message
            1
        | e ->
            Log.Fatal(e, "Application Startup failed")
            2
    finally Log.CloseAndFlush()