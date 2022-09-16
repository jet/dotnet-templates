module ReactorTemplate.SourceArgs

open Argu
open Serilog
open System

type Configuration(tryGet) =
    inherit Args.Configuration(tryGet)
#if !sourceKafka
    member _.DynamoIndexTable =             tryGet Args.INDEX_TABLE
#endif

#if !sourceKafka
module Cosmos =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float

        | [<AltCommandLine "-a"; Unique>]   LeaseContainer of string
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-b"; Unique>]   MaxItems of int
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
        
#if (kafka && blank)
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<Args.KafkaSinkParameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<Args.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<Args.Dynamo.Parameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request Verbose Logging from ChangeFeedProcessor and Store. Default: off"
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 5."
                | Retries _ ->              "specify operation retries. Default: 9."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds. Default: 30."

                | LeaseContainer _ ->       "specify Container Name (in this [target] Database) for Leases container. Default: `SourceContainer` + `-aux`."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxItems _ ->             "maximum item count to supply for the Change Feed query. Default: use response size limit"
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: 1"

#if (kafka && blank)
                | Kafka _ ->                "Kafka Sink parameters."
#else
                | Cosmos _ ->               "CosmosDb Sink parameters."
                | Dynamo _ ->               "DynamoDb Sink parameters."
#endif
    type Arguments(c : Args.Configuration, p : ParseResults<Parameters>) =
        let discovery =                     p.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection) |> Equinox.CosmosStore.Discovery.ConnectionString
        let mode =                          p.TryGetResult ConnectionMode
        let timeout =                       p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 9)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 30.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.TryGetResult Database  |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let containerId =                   p.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        let leaseContainerId =              p.GetResult(LeaseContainer, containerId + "-aux")
        let fromTail =                      p.Contains FromTail
        let maxItems =                      p.TryGetResult MaxItems
        let tailSleepInterval =             TimeSpan.FromMilliseconds 500.
        let lagFrequency =                  p.GetResult(LagFreqM, 1.) |> TimeSpan.FromMinutes
        member _.Verbose =                  p.Contains Verbose
        member private _.ConnectLeases() =  connector.CreateUninitialized(database, leaseContainerId)
        member x.MonitoringParams(log : ILogger) =
            let leases : Microsoft.Azure.Cosmos.Container = x.ConnectLeases()
            log.Information("ChangeFeed Leases Database {db} Container {container}. MaxItems limited to {maxItems}",
                leases.Database.Id, leases.Id, Option.toNullable maxItems)
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            (leases, fromTail, maxItems, tailSleepInterval, lagFrequency)
        member x.ConnectStoreAndMonitored() =
            connector.ConnectStoreAndMonitored(database, containerId)
#if (kafka && blank)
#if kafka            
        member val Kafka =
            match p.GetSubCommand() with
            | Kafka kafka -> Args.KafkaSinkArguments(c, kafka)
            | _ -> Args.missingArg "Must specify `kafka` arguments"
#endif            
        member x.ConnectTarget(_cache) = ()
#else
        member private _.TargetStoreArgs : Args.TargetStoreArgs =
            match p.GetSubCommand() with
            | Cosmos cosmos -> Args.TargetStoreArgs.Cosmos (Args.Cosmos.Arguments(c, cosmos))
            | Dynamo dynamo -> Args.TargetStoreArgs.Dynamo (Args.Dynamo.Arguments(c, dynamo))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` target store when source is `esdb`"
        member x.ConnectTarget(cache) : Config.Store =
            Args.TargetStoreArgs.connectTarget x.TargetStoreArgs cache
#if kafka            
        member x.Kafka =
            match x.TargetStoreArgs with
            | Args.TargetStoreArgs.Cosmos cosmos -> cosmos.Kafka
            | Args.TargetStoreArgs.Dynamo dynamo -> dynamo.Kafka
#endif            
#endif

module Dynamo =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-s">]           ServiceUrl of string
        | [<AltCommandLine "-sa">]          AccessKey of string
        | [<AltCommandLine "-ss">]          SecretKey of string
        | [<AltCommandLine "-t">]           Table of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesTimeoutS of float
        | [<AltCommandLine "-i">]           IndexTable of string
        | [<AltCommandLine "-is">]          IndexSuffix of string
        | [<AltCommandLine "-b"; Unique>]   MaxItems of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        | [<AltCommandLine "-d">]           StreamsDop of int
#if (kafka && blank)
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<Args.KafkaSinkParameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<Args.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<Args.Dynamo.Parameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "Include low level Store logging."
                | ServiceUrl _ ->           "specify a server endpoint for a Dynamo account. (optional if environment variable " + Args.SERVICE_URL + " specified)"
                | AccessKey _ ->            "specify an access key id for a Dynamo account. (optional if environment variable " + Args.ACCESS_KEY + " specified)"
                | SecretKey _ ->            "specify a secret access key for a Dynamo account. (optional if environment variable " + Args.SECRET_KEY + " specified)"
                | Retries _ ->              "specify operation retries (default: 9)."
                | RetriesTimeoutS _ ->      "specify max wait-time including retries in seconds (default: 60)"
                | Table _ ->                "specify a table name for the primary store. (optional if environment variable " + Args.TABLE + " specified)"
                | IndexTable _ ->           "specify a table name for the index store. (optional if environment variable " + Args.INDEX_TABLE + " specified. default: `Table`+`IndexSuffix`)"
                | IndexSuffix _ ->          "specify a suffix for the index store. (optional if environment variable " + Args.INDEX_TABLE + " specified. default: \"-index\")"
                | MaxItems _ ->             "maximum events to load in a batch. Default: 100"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | StreamsDop _ ->           "parallelism when loading events from Store Feed Source. Default 4"
#if (kafka && blank)
                | Kafka _ ->                "Kafka Sink parameters."
#else
                | Cosmos _ ->               "CosmosDb Sink parameters."
                | Dynamo _ ->               "DynamoDb Sink parameters."
#endif

    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let serviceUrl =                    p.TryGetResult ServiceUrl |> Option.defaultWith (fun () -> c.DynamoServiceUrl)
        let accessKey =                     p.TryGetResult AccessKey  |> Option.defaultWith (fun () -> c.DynamoAccessKey)
        let secretKey =                     p.TryGetResult SecretKey  |> Option.defaultWith (fun () -> c.DynamoSecretKey)
        let table =                         p.TryGetResult Table      |> Option.defaultWith (fun () -> c.DynamoTable)
        let indexSuffix =                   p.GetResult(IndexSuffix, "-index")
        let indexTable =                    p.TryGetResult IndexTable |> Option.orElseWith  (fun () -> c.DynamoIndexTable) |> Option.defaultWith (fun () -> table + indexSuffix)
        let fromTail =                      p.Contains FromTail
        let tailSleepInterval =             TimeSpan.FromMilliseconds 500.
        let batchSizeCutoff =               p.GetResult(MaxItems, 100)
        let streamsDop =                    p.GetResult(StreamsDop, 4)
        let timeout =                       p.GetResult(RetriesTimeoutS, 60.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 9)
        let connector =                     Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let client =                        connector.CreateClient()
        let indexStoreClient =              lazy client.ConnectStore("Index", indexTable)
        member val Verbose =                p.Contains Verbose
        member _.Connect() =                connector.LogConfiguration()
                                            client.ConnectStore("Main", table) |> DynamoStoreContext.create
        member _.MonitoringParams(log : ILogger) =
            log.Information("DynamoStoreSource BatchSizeCutoff {batchSizeCutoff} Hydrater parallelism {streamsDop}", batchSizeCutoff, streamsDop)
            let indexStoreClient = indexStoreClient.Value
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            indexStoreClient, fromTail, batchSizeCutoff, tailSleepInterval, streamsDop
        member _.CreateCheckpointStore(group, cache) =
            let indexTable = indexStoreClient.Value
            indexTable.CreateCheckpointService(group, cache, Config.log)
#if (kafka && blank)
        member x.ConnectTarget(_cache) = ()
#if kafka        
        member val Kafka =
            match p.GetSubCommand() with
            | Kafka kafka -> Args.KafkaSinkArguments(c, kafka)
            | _ -> Args.missingArg "Must specify `kafka` arguments"
#endif            
#else            
        member private _.TargetStoreArgs : Args.TargetStoreArgs =
            match p.GetSubCommand() with
            | Cosmos cosmos -> Args.TargetStoreArgs.Cosmos (Args.Cosmos.Arguments(c, cosmos))
            | Dynamo dynamo -> Args.TargetStoreArgs.Dynamo (Args.Dynamo.Arguments(c, dynamo))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` target store when source is `esdb`"
        member x.ConnectTarget(cache) : Config.Store =
            Args.TargetStoreArgs.connectTarget x.TargetStoreArgs cache
#if kafka
        member x.Kafka =
            match x.TargetStoreArgs with
            | Args.TargetStoreArgs.Cosmos cosmos -> cosmos.Kafka
            | Args.TargetStoreArgs.Dynamo dynamo -> dynamo.Kafka
#endif
#endif

module Esdb =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-c">]           Connection of string
        | [<AltCommandLine "-p"; Unique>]   Credentials of string
        | [<AltCommandLine "-o">]           Timeout of float
        | [<AltCommandLine "-r">]           Retries of int

        | [<AltCommandLine "-b"; Unique>]   BatchSize of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<Args.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<Args.Dynamo.Parameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "Include low level Store logging."
                | Connection _ ->           "EventStore Connection String. (optional if environment variable EQUINOX_ES_CONNECTION specified)"
                | Credentials _ ->          "Credentials string for EventStore (used as part of connection string, but NOT logged). Default: use EQUINOX_ES_CREDENTIALS environment variable (or assume no credentials)"
                | Timeout _ ->              "specify operation timeout in seconds. Default: 20."
                | Retries _ ->              "specify operation retries. Default: 3."

                | FromTail ->               "Start the processing from the Tail"
                | BatchSize _ ->            "maximum events to load in a batch. Default: 100"
                
                | Cosmos _ ->               "CosmosDB Target Store parameters (also used for checkpoint storage)."
                | Dynamo _ ->               "DynamoDB Target Store parameters (also used for checkpoint storage)."
    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let startFromTail =                 p.Contains FromTail
        let batchSize =                     p.GetResult(BatchSize, 100)
        let tailSleepInterval =             TimeSpan.FromSeconds 0.5
        let connectionStringLoggable =      p.TryGetResult Connection |> Option.defaultWith (fun () -> c.EventStoreConnection)
        let credentials =                   p.TryGetResult Credentials |> Option.orElseWith (fun () -> c.MaybeEventStoreCredentials)
        let discovery =                     match credentials with Some x -> String.Join(";", connectionStringLoggable, x) | None -> connectionStringLoggable
                                            |> Equinox.EventStoreDb.Discovery.ConnectionString
        let retries =                       p.GetResult(Retries, 3)
        let timeout =                       p.GetResult(Timeout, 20.) |> TimeSpan.FromSeconds
        let checkpointInterval =            TimeSpan.FromHours 1.
        member val Verbose =                p.Contains Verbose

        member _.Connect(log : ILogger, appName, nodePreference) : Equinox.EventStoreDb.EventStoreConnection =
            log.Information("EventStore {discovery}", connectionStringLoggable)
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
            Equinox.EventStoreDb.EventStoreConnector(timeout, retries, tags = tags)
                .Establish(appName, discovery, Equinox.EventStoreDb.ConnectionStrategy.ClusterSingle nodePreference)
    
        member _.MonitoringParams(log : ILogger) =
            log.Information("EventStoreSource BatchSize {batchSize} ", batchSize)
            startFromTail, batchSize, tailSleepInterval
        member _.CreateCheckpointStore(group, store : Config.Store) : Propulsion.Feed.IFeedCheckpointStore =
            Args.Checkpoints.createCheckpointStore (group, checkpointInterval, store) 
        member private _.TargetStoreArgs : Args.TargetStoreArgs =
            match p.GetSubCommand() with
            | Cosmos cosmos -> Args.TargetStoreArgs.Cosmos (Args.Cosmos.Arguments(c, cosmos))
            | Dynamo dynamo -> Args.TargetStoreArgs.Dynamo (Args.Dynamo.Arguments(c, dynamo))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` target store when source is `esdb`"
#if kafka
        member x.Kafka =
            match x.TargetStoreArgs with
            | Args.TargetStoreArgs.Cosmos cosmos -> cosmos.Kafka
            | Args.TargetStoreArgs.Dynamo dynamo -> dynamo.Kafka
#endif            
        member x.ConnectTarget(cache) : Config.Store =
            Args.TargetStoreArgs.connectTarget x.TargetStoreArgs cache

module Sss =
    
    // TOCONSIDER: add DB connectors other than MsSql
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-t"; Unique>]   Tail of intervalS: float
        | [<AltCommandLine "-c"; Unique>]   Connection of string
        | [<AltCommandLine "-p"; Unique>]   Credentials of string
        | [<AltCommandLine "-s">]           Schema of string
        
        | [<AltCommandLine "-b"; Unique>]   BatchSize of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail

        | [<AltCommandLine "-cc"; Unique>]  CheckpointsConnection of string
        | [<AltCommandLine "-cp"; Unique>]  CheckpointsCredentials of string

        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<Args.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<Args.Dynamo.Parameters>
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Tail _ ->                 "Polling interval in Seconds. Default: 1"
                | BatchSize _ ->            "Maximum events to request from feed. Default: 512"
                | Connection _ ->           "Connection string for SqlStreamStore db. Optional if SQLSTREAMSTORE_CONNECTION specified"
                | Credentials _ ->          "Credentials string for SqlStreamStore db (used as part of connection string, but NOT logged). Default: use SQLSTREAMSTORE_CREDENTIALS environment variable (or assume no credentials)"
                | Schema _ ->               "Database schema name"
                | FromTail ->               "Start the processing from the Tail"
                | CheckpointsConnection _ ->"Connection string for Checkpoints sql db. Optional if SQLSTREAMSTORE_CONNECTION_CHECKPOINTS specified. Default: same as `Connection`"
                | CheckpointsCredentials _ ->"Credentials string for Checkpoints sql db. (used as part of checkpoints connection string, but NOT logged). Default (when no `CheckpointsConnection`: use `Credentials. Default (when `CheckpointsConnection` specified): use SQLSTREAMSTORE_CREDENTIALS_CHECKPOINTS environment variable (or assume no credentials)"
                | Cosmos _ ->               "CosmosDB Target Store parameters"
                | Dynamo _ ->               "DynamoDB Target Store parameters"

    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let startFromTail =                 p.Contains FromTail
        let tailSleepInterval =             p.GetResult(Tail, 1.) |> TimeSpan.FromSeconds
        let checkpointEventInterval =       TimeSpan.FromHours 1. // Ignored when storing to Propulsion.SqlStreamStore.ReaderCheckpoint
        let batchSize =                     p.GetResult(BatchSize, 512)
        let connection =                    p.TryGetResult Connection |> Option.defaultWith (fun () -> c.SqlStreamStoreConnection)
        let credentials =                   p.TryGetResult Credentials |> Option.orElseWith (fun () -> c.SqlStreamStoreCredentials) |> Option.toObj
        let schema =                        p.GetResult(Schema, null)

        member x.BuildCheckpointsConnectionString() =
            let c, cs =
                match p.TryGetResult CheckpointsConnection, p.TryGetResult CheckpointsCredentials with
                | Some c, Some p -> c, String.Join(";", c, p)
                | None, Some p ->   let c = connection in c, String.Join(";", c, p)
                | None, None ->     let c = connection in c, String.Join(";", c, credentials)
                | Some cc, None ->  let p = c.SqlStreamStoreCredentialsCheckpoints |> Option.toObj
                                    cc, String.Join(";", cc, p)
            Log.Information("Checkpoints MsSql Connection {connectionString}", c)
            cs
        member x.Connect() =
            let conn, creds, schema, autoCreate = connection, credentials, schema, false
            let sssConnectionString = String.Join(";", conn, creds)
            Log.Information("SqlStreamStore MsSql Connection {connectionString} Schema {schema} AutoCreate {autoCreate}", conn, schema, autoCreate)
            let rawStore = Equinox.SqlStreamStore.MsSql.Connector(sssConnectionString, schema, autoCreate=autoCreate).Connect() |> Async.RunSynchronously
            Equinox.SqlStreamStore.SqlStreamStoreConnection rawStore
        member _.MonitoringParams(log : ILogger) =
            log.Information("SqlStreamStoreSource BatchSize {batchSize} ", batchSize)
            startFromTail, batchSize, tailSleepInterval
        member private _.TargetStoreArgs : Args.TargetStoreArgs =
            match p.GetSubCommand() with
            | Cosmos cosmos -> Args.TargetStoreArgs.Cosmos (Args.Cosmos.Arguments(c, cosmos))
            | Dynamo dynamo -> Args.TargetStoreArgs.Dynamo (Args.Dynamo.Arguments(c, dynamo))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` target store when source is `sss`"
        member x.CreateCheckpointStoreSql(groupName) : Propulsion.Feed.IFeedCheckpointStore =
            let connectionString = x.BuildCheckpointsConnectionString()
            Propulsion.SqlStreamStore.ReaderCheckpoint.Service(connectionString, groupName, checkpointEventInterval)
#if kafka
        member x.Kafka =
            match x.TargetStoreArgs with
            | Args.TargetStoreArgs.Cosmos cosmos -> cosmos.Kafka
            | Args.TargetStoreArgs.Dynamo dynamo -> dynamo.Kafka
#endif            
        member x.ConnectTarget(cache) : Config.Store =
            Args.TargetStoreArgs.connectTarget x.TargetStoreArgs cache

#else // sourceKafka
module Kafka =
        
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        | [<AltCommandLine "-m"; Unique>]   MaxInflightMb of float
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float
#if (kafka && blank)
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<Args.KafkaSinkParameters>
#else
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Cosmos of ParseResults<Args.Cosmos.Parameters>
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Dynamo of ParseResults<Args.Dynamo.Parameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
                | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
                | MaxInflightMb _ ->        "maximum MiB of data to read ahead. Default: 10."
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: 1."
#if (kafka && blank)
                | Kafka _ ->                "Kafka Sink parameters."
#else
                | Cosmos _ ->               "CosmosDb Sink parameters."
                | Dynamo _ ->               "CosmosDb Sink parameters."
#endif
    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        member val Broker =                 p.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  p.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member val MaxInFlightBytes =       p.GetResult(MaxInflightMb, 10.) * 1024. * 1024. |> int64
        member val LagFrequency =           p.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member x.BuildSourceParams() =      x.Broker, x.Topic

#if (kafka && blank)
        member x.ConnectTarget(_cache) = ()
#if kafka            
        member val Kafka =
             match p.GetSubCommand() with
             | Kafka kafka -> Args.KafkaSinkArguments(c, kafka)
             | _ -> Args.missingArg "Must specify kafka arguments"
#endif             
#else
        member private _.TargetStoreArgs : Args.TargetStoreArgs =
            match p.GetSubCommand() with
            | Cosmos cosmos -> Args.TargetStoreArgs.Cosmos (Args.Cosmos.Arguments(c, cosmos))
            | Dynamo dynamo -> Args.TargetStoreArgs.Dynamo (Args.Dynamo.Arguments(c, dynamo))
            | _ -> Args.missingArg "Must specify `cosmos` or `dynamo` target store when source is `kafka`"
#if kafka            
        member x.Kafka =
            match x.TargetStoreArgs with
            | Args.TargetStoreArgs.Cosmos cosmos -> cosmos.Sink
            | Args.TargetStoreArgs.Dynamo dynamo -> dynamo.Sink
#endif            
        member x.ConnectTarget(cache) : Config.Store =
            Args.TargetStoreArgs.connectTarget x.TargetStoreArgs cache
#endif
#endif
