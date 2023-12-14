module TestbedTemplate.Storage

open Argu
open System

type Configuration(tryGet: string -> string option) =

    let get key = match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"

//#if (memoryStore || (!cosmos && !eventStore))
module MemoryStore =
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]       Verbose
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->       "Include low level Store logging."
    let config () =
        Store.Config.Memory (Equinox.MemoryStore.VolatileStore())

//#endif
//#if cosmos
module Cosmos =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]       Verbose
        | [<AltCommandLine "-m">]       ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">]       Timeout of float
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-rt">]      RetriesWaitTime of float
        | [<AltCommandLine "-s">]       Connection of string
        | [<AltCommandLine "-d">]       Database of string
        | [<AltCommandLine "-c">]       Container of string
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->       "Include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                | Retries _ ->          "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
                | ConnectionMode _ ->   "override the connection mode. Default: Direct."
                | Connection _ ->       "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->         "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->        "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
    type Arguments(c: Configuration, p: ParseResults<Parameters>) =
        let discovery =                     p.GetResult(Connection, fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let timeout =                       p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.GetResult(Database, fun () -> c.CosmosDatabase)
        let container =                     p.GetResult(Container, fun () -> c.CosmosContainer)
        member _.Connect(tipMaxEvents, queryMaxItems) = connector.ConnectContext("Main", database, container, tipMaxEvents, queryMaxItems)


    // Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    // 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and container named "equinox-test"
    // 2) Set the 3x environment variables and create a local Equinox using tools/Equinox.Tool/bin/Release/net461/eqx.exe `
    //     init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER

    let config (cache, unfolds, maxItems) (info: Arguments) =
        let context = info.Connect(tipMaxEvents = 256, queryMaxItems = maxItems) |> Async.RunSynchronously
        let cacheStrategy =
            if cache then
                let c = Equinox.Cache("TestbedTemplate", sizeMb = 50)
                Equinox.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            else Equinox.CachingStrategy.NoCaching
        Store.Config.Cosmos (context, cacheStrategy, unfolds)

//#endif
//#if eventStore
/// To establish a local node to run the tests against:
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
module EventStore =
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]       Verbose
        | [<AltCommandLine "-o">]       Timeout of float
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-h">]       ConnectionString of string
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->            "include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                | Retries _ ->          "specify operation retries. Default: 1."
                | ConnectionString _ -> "esdb connection string"

    open Equinox.EventStoreDb

    type Arguments(p: ParseResults<Parameters>) =
        member val ConnectionString =   p.GetResult(ConnectionString)

        member val Retries =            p.GetResult(Retries, 1)
        member val Timeout =            p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds

    let private connect (log: Serilog.ILogger) connectionString (operationTimeout, operationRetries) =
        EventStoreConnector(reqTimeout=operationTimeout,// reqRetries=operationRetries,
                // heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit=col,
                // log = (if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
                tags = ["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
            .Establish("TestbedTemplate", Discovery.ConnectionString connectionString, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let private createContext connection batchSize = EventStoreContext(connection, batchSize = batchSize)
    let config (log: Serilog.ILogger, storeLog) (cache, unfolds, batchSize) (args: ParseResults<Parameters>) =
        let a = Arguments args
        let timeout, retries as operationThrottling = a.Timeout, a.Retries
        log.Information("EventStore {connectionString} timeout: {timeout}s retries {retries}",
            a.ConnectionString, timeout.TotalSeconds, retries)
        let conn = connect storeLog a.ConnectionString operationThrottling
        let cacheStrategy =
            if cache then
                let c = Equinox.Cache("TestbedTemplate", sizeMb = 50)
                Equinox.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            else Equinox.CachingStrategy.NoCaching
        Store.Config.Esdb ((createContext conn batchSize), cacheStrategy, unfolds)
//#endif
