module Fc.Web.Program

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj
    let set varName value : unit = Environment.SetEnvironmentVariable(varName, value)

module Configuration =

    let private initEnvVar var key loadF =
        if None = EnvVar.tryGet var then
            printfn "Setting %s from %A" var key
            EnvVar.set var (loadF key)

    let initialize () =
        // e.g. initEnvVar     "EQUINOX_COSMOS_CONTAINER"    "CONSUL KEY" readFromConsul
        () // TODO add any custom logic preprocessing commandline arguments and/or gathering custom defaults from external sources, etc

module Args =

    exception MissingArg of string
    let private getEnvVarForArgumentOrThrow varName argName =
        match EnvVar.tryGet varName with
        | None -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
        | Some x -> x
    let private defaultWithEnvVar varName argName = function
        | None -> getEnvVarForArgumentOrThrow varName argName
        | Some x -> x
    let isEnvVarTrue varName = EnvVar.tryGet varName |> Option.exists (fun s -> String.Equals(s, bool.TrueString, StringComparison.OrdinalIgnoreCase))
    open Argu
    open Equinox.EventStore
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose

        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Es of ParseResults<EsParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Verbose ->                "request Verbose Logging. Default: off."
                | Es _ ->                   "specify EventStore input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.Verbose =                 a.Contains Parameters.Verbose
        member __.StatsInterval =           TimeSpan.FromMinutes 1.

        member val Source : EsArguments =
            match a.TryGetSubCommand() with
            | Some (Es es) -> (EsArguments es)
            | _ -> raise (MissingArg "Must specify one of cosmos or es for Src")
    and [<NoEquality; NoComparison>] EsParameters =
        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-oh">]          HeartbeatTimeout of float
        | [<AltCommandLine "-T">]           Tcp
        | [<AltCommandLine "-h">]           Host of string
        | [<AltCommandLine "-x">]           Port of int
        | [<AltCommandLine "-u">]           Username of string
        | [<AltCommandLine "-p">]           Password of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Verbose ->                "Include low level Store logging."
                | Tcp ->                    "Request connecting direct to a TCP/IP endpoint. Default: Use Clustered mode with Gossip-driven discovery (unless environment variable EQUINOX_ES_TCP specifies 'true')."
                | Host _ ->                 "TCP mode: specify a hostname to connect to directly. Clustered mode: use Gossip protocol against all A records returned from DNS query. (optional if environment variable EQUINOX_ES_HOST specified)"
                | Port _ ->                 "specify a custom port. Uses value of environment variable EQUINOX_ES_PORT if specified. Defaults for Cluster and Direct TCP/IP mode are 30778 and 1113 respectively."
                | Username _ ->             "specify a username. (optional if environment variable EQUINOX_ES_USERNAME specified)"
                | Password _ ->             "specify a Password. (optional if environment variable EQUINOX_ES_PASSWORD specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds. Default: 1.5."
    and EsArguments(a : ParseResults<EsParameters>) =
        member __.Discovery =
            match __.Tcp, __.Port with
            | false, None ->   Discovery.GossipDns            __.Host
            | false, Some p -> Discovery.GossipDnsCustomPort (__.Host, p)
            | true, None ->    Discovery.Uri                 (UriBuilder("tcp", __.Host, 1113).Uri)
            | true, Some p ->  Discovery.Uri                 (UriBuilder("tcp", __.Host, p).Uri)
        member __.Tcp =                     a.Contains Tcp || isEnvVarTrue "EQUINOX_ES_TCP"
        member __.Port =                    match a.TryGetResult Port with Some x -> Some x | None -> EnvVar.tryGet "EQUINOX_ES_PORT" |> Option.map int
        member __.Host =                    a.TryGetResult Host     |> defaultWithEnvVar "EQUINOX_ES_HOST"     "Host"
        member __.User =                    a.TryGetResult Username |> defaultWithEnvVar "EQUINOX_ES_USERNAME" "Username"
        member __.Password =                a.TryGetResult Password |> defaultWithEnvVar "EQUINOX_ES_PASSWORD" "Password"
        member __.Retries =                 a.GetResult(EsParameters.Retries, 3)
        member __.Timeout =                 a.GetResult(EsParameters.Timeout, 20.) |> TimeSpan.FromSeconds
        member __.Heartbeat =               a.GetResult(HeartbeatTimeout, 1.5) |> TimeSpan.FromSeconds
        member x.Connect(log: ILogger, storeLog: ILogger, appName, connectionStrategy) =
            let s (x : TimeSpan) = x.TotalSeconds
            let discovery = x.Discovery
            log.ForContext("host", x.Host).ForContext("port", x.Port)
                .Information("EventStore {discovery} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                    discovery, s x.Heartbeat, s x.Timeout, x.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Connector(x.User, x.Password, x.Timeout, x.Retries, log=log, heartbeatTimeout=x.Heartbeat, tags=tags)
                .Establish(appName, discovery, connectionStrategy) |> Async.RunSynchronously

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

module Logging =

    let initialize verbose =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
                .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()

let [<Literal>] AppName = "Fc.Web"

/// Defines the Hosting configuration, including registration of the store and backend services
type Startup() =

    // This method gets called by the runtime. Use this method to add services to the container.
    member __.ConfigureServices(services: IServiceCollection) : unit =
        services
            .AddMvc()
            .SetCompatibilityVersion(CompatibilityVersion.Latest)
            .AddNewtonsoftJson() // until FsCodec.SystemTextJson is available
            |> ignore

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member __.Configure(app: IApplicationBuilder, env: IHostEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseRouting()
            .UseSerilogRequestLogging() // see https://nblumhardt.com/2019/10/serilog-in-aspnetcore-3/
            .UseEndpoints(fun endpoints -> endpoints.MapControllers() |> ignore)
            |> ignore

let build (args : Args.Arguments) =
    let cache = Equinox.Cache(AppName, sizeMb=10)
    let create =
        let es = args.Source
        let connection = es.Connect(Log.Logger, Log.Logger, AppName, Equinox.EventStore.ConnectionStrategy.ClusterSingle Equinox.EventStore.NodePreference.Master)
        let context = Equinox.EventStore.Context(connection, Equinox.EventStore.BatchingPolicy(maxBatchSize=500))
        Fc.Domain.StockProcessManager.EventStore.create (context, cache)
    let inventoryId = InventoryId.parse "FC000"
    create inventoryId (1000, 5, 3)

let run argv args =
    let processManager = build args
    WebHost
        .CreateDefaultBuilder(argv)
        .UseSerilog()
        .ConfigureServices(fun svc -> svc.AddSingleton(processManager) |> ignore)
        .UseStartup<Startup>()
        .Build()
        .Run()

[<EntryPoint>]
let main argv =
    try let args = Args.parse argv
        try Logging.initialize args.Verbose
            try Configuration.initialize ()
                run argv args
                0
            with e when not (e :? Args.MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with Args.MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
