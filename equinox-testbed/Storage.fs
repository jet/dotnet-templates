module TestbedTemplate.Storage

open Argu
open System

exception MissingArg of string

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type StorageConfig =
//#if (memoryStore || (!cosmos && !eventStore))
    | Memory of Equinox.MemoryStore.VolatileStore
//#endif
//#if eventStore
    | Es of Equinox.EventStore.Context * Equinox.EventStore.CachingStrategy option * unfolds: bool
//#endif
//#if cosmos
    | Cosmos of Equinox.Cosmos.Gateway * Equinox.Cosmos.CachingStrategy * unfolds: bool * databaseId: string * containerId: string
//#endif
    
//#if (memoryStore || (!cosmos && !eventStore))
module MemoryStore =
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine("-vs")>] VerboseStore
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | VerboseStore ->       "Include low level Store logging."
    let config () =
        StorageConfig.Memory (Equinox.MemoryStore.VolatileStore())

//#endif
//#if cosmos
module Cosmos =
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argument or via the %s environment variable" msg key)
        | x -> x

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine("-vs")>] VerboseStore
        | [<AltCommandLine("-cm")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-rt")>] RetriesWaitTime of int
        | [<AltCommandLine("-s")>] Connection of string
        | [<AltCommandLine("-d")>] Database of string
        | [<AltCommandLine("-c")>] Container of string
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | VerboseStore ->       "Include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                | Retries _ ->          "specify operation retries. Default: 1."
                | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 5."
                | ConnectionMode _ ->   "override the connection mode. Default: Direct."
                | Connection _ ->       "specify a connection string for a Cosmos account. Default: envvar:EQUINOX_COSMOS_CONNECTION."
                | Database _ ->         "specify a database name for store. Default: envvar:EQUINOX_COSMOS_DATABASE."
                | Container _ ->        "specify a container name for store. Default: envvar:EQUINOX_COSMOS_CONTAINER."
    type Arguments(a : ParseResults<Parameters>) =
        member __.Mode =                a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.Direct)
        member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Container =           match a.TryGetResult Container   with Some x -> x | None -> envBackstop "Container"  "EQUINOX_COSMOS_CONTAINER"

        member __.Timeout =             a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries,1)
        member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5)

    /// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    /// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and container named "equinox-test"
    /// 2) Set the 3x environment variables and create a local Equinox using tools/Equinox.Tool/bin/Release/net461/eqx.exe `
    ///     init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
    open Equinox.Cosmos
    open Serilog

    let private createGateway connection maxItems = Gateway(connection, BatchingPolicy(defaultMaxItems=maxItems))
    let private context (log: ILogger, storeLog: ILogger) (a : Arguments) =
        let (Discovery.UriAndKey (endpointUri,_)) as discovery = a.Connection|> Discovery.FromConnectionString
        log.Information("CosmosDb {mode} {connection} Database {database} Container {container}",
            a.Mode, endpointUri, a.Database, a.Container)
        Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
            (let t = a.Timeout in t.TotalSeconds), a.Retries, a.MaxRetryWaitTime)
        let connector = Connector(a.Timeout, a.Retries, a.MaxRetryWaitTime, storeLog, mode=a.Mode)
        discovery, a.Database, a.Container, connector
    let config (log: ILogger, storeLog) (cache, unfolds, batchSize) info =
        let discovery, dbName, containerName, connector = context (log, storeLog) info
        let conn = connector.Connect("TestbedTemplate", discovery) |> Async.RunSynchronously
        let cacheStrategy =
            if cache then
                let c = Caching.Cache("TestbedTemplate", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            else CachingStrategy.NoCaching
        StorageConfig.Cosmos (createGateway conn batchSize, cacheStrategy, unfolds, dbName, containerName)

//#endif
//#if eventStore
/// To establish a local node to run the tests against:
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
module EventStore =
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine("-vs")>] VerboseStore
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-g")>] Host of string
        | [<AltCommandLine("-u")>] Username of string
        | [<AltCommandLine("-p")>] Password of string
        | [<AltCommandLine("-c")>] ConcurrentOperationsLimit of int
        | [<AltCommandLine("-h")>] HeartbeatTimeout of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | VerboseStore ->       "include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds. Default: 5."
                | Retries _ ->          "specify operation retries. Default: 1."
                | Host _ ->             "specify a DNS query, using Gossip-driven discovery against all A records returned. Default: localhost."
                | Username _ ->         "specify a username. Default: admin."
                | Password _ ->         "specify a Password. Default: changeit."
                | ConcurrentOperationsLimit _ -> "max concurrent operations in flight. Default: 5000."
                | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds. Default: 1.5."

    open Equinox.EventStore

    type Arguments(a : ParseResults<Parameters>) =
        member __.Host =                a.GetResult(Host,"localhost")
        member __.Credentials =         a.GetResult(Username,"admin"), a.GetResult(Password,"changeit")

        member __.Timeout =             a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries, 1)
        member __.HeartbeatTimeout =    a.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
        member __.ConcurrentOperationsLimit = a.GetResult(ConcurrentOperationsLimit,5000)

    let private connect (log: Serilog.ILogger) (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
        Connector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries,
                heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col,
                log=(if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
                tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
            .Establish("TestbedTemplate", Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let private createContext connection batchSize = Context(connection, BatchingPolicy(maxBatchSize = batchSize))
    let config (log: Serilog.ILogger, storeLog) (cache, unfolds, batchSize) (args : ParseResults<Parameters>) =
        let a = Arguments(args)
        let (timeout, retries) as operationThrottling = a.Timeout, a.Retries
        let heartbeatTimeout = a.HeartbeatTimeout
        let concurrentOperationsLimit = a.ConcurrentOperationsLimit
        log.Information("EventStore {host} heartbeat: {heartbeat}s timeout: {timeout}s concurrent reqs: {concurrency} retries {retries}",
            a.Host, heartbeatTimeout.TotalSeconds, timeout.TotalSeconds, concurrentOperationsLimit, retries)
        let conn = connect storeLog (a.Host, heartbeatTimeout, concurrentOperationsLimit) a.Credentials operationThrottling |> Async.RunSynchronously
        let cacheStrategy =
            if cache then
                let c = Caching.Cache("TestbedTemplate", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        StorageConfig.Es ((createContext conn batchSize), cacheStrategy, unfolds)
//#endif