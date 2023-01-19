module Reactor.SourceArgs

open Argu
open Infrastructure // Args etc
open Serilog
open System

module Config = Domain.Config

type Configuration(tryGet) =
    inherit Args.Configuration(tryGet)
    member _.DynamoIndexTable =             tryGet Args.Configuration.Dynamo.INDEX_TABLE

module Dynamo =

    open Args.Configuration.Dynamo
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-sr">]          RegionProfile of string
        | [<AltCommandLine "-su">]          ServiceUrl of string
        | [<AltCommandLine "-sa">]          AccessKey of string
        | [<AltCommandLine "-ss">]          SecretKey of string
        | [<AltCommandLine "-t">]           Table of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesTimeoutS of float
        | [<AltCommandLine "-i">]           IndexTable of string
        | [<AltCommandLine "-is">]          IndexSuffix of string
        | [<AltCommandLine "-b"; Unique>]   MaxItems of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | RegionProfile _ ->        "specify an AWS Region (aka System Name, e.g. \"us-east-1\") to connect to using the implicit AWS SDK/tooling config and/or environment variables etc. Optional if:\n" +
                                            "1) $" + REGION + " specified OR\n" +
                                            "2) Explicit `ServiceUrl`/$" + SERVICE_URL + "+`AccessKey`/$" + ACCESS_KEY + "+`Secret Key`/$" + SECRET_KEY + " specified.\n" +
                                            "See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html for details"
                | ServiceUrl _ ->           "specify a server endpoint for a Dynamo account. (optional if environment variable " + SERVICE_URL + " specified)"
                | AccessKey _ ->            "specify an access key id for a Dynamo account. (optional if environment variable " + ACCESS_KEY + " specified)"
                | SecretKey _ ->            "specify a secret access key for a Dynamo account. (optional if environment variable " + SECRET_KEY + " specified)"
                | Retries _ ->              "specify operation retries (default: 9)."
                | RetriesTimeoutS _ ->      "specify max wait-time including retries in seconds (default: 60)"
                | Table _ ->                "specify a table name for the primary store. (optional if environment variable " + TABLE + " specified)"
                | IndexTable _ ->           "specify a table name for the index store. (optional if environment variable " + INDEX_TABLE + " specified. default: `Table`+`IndexSuffix`)"
                | IndexSuffix _ ->          "specify a suffix for the index store. (optional if environment variable " + INDEX_TABLE + " specified. default: \"-index\")"
                | MaxItems _ ->             "maximum events to load in a batch. Default: 100"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."

    type Arguments(c : Args.Configuration, p : ParseResults<Parameters>) =
        let conn =                          match p.TryGetResult RegionProfile |> Option.orElseWith (fun () -> c.DynamoRegion) with
                                            | Some systemName ->
                                                Choice1Of2 systemName
                                            | None ->
                                                let serviceUrl =  p.TryGetResult ServiceUrl |> Option.defaultWith (fun () -> c.DynamoServiceUrl)
                                                let accessKey =   p.TryGetResult AccessKey  |> Option.defaultWith (fun () -> c.DynamoAccessKey)
                                                let secretKey =   p.TryGetResult SecretKey  |> Option.defaultWith (fun () -> c.DynamoSecretKey)
                                                Choice2Of2 (serviceUrl, accessKey, secretKey)
        let connector =                     let timeout = p.GetResult(RetriesTimeoutS, 60.) |> TimeSpan.FromSeconds
                                            let retries = p.GetResult(Retries, 9)
                                            match conn with
                                            | Choice1Of2 systemName ->
                                                Equinox.DynamoStore.DynamoStoreConnector(systemName, timeout, retries)
                                            | Choice2Of2 (serviceUrl, accessKey, secretKey) ->
                                                Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let table =                         p.TryGetResult Table      |> Option.defaultWith (fun () -> c.DynamoTable)
        let indexSuffix =                   p.GetResult(IndexSuffix, "-index")
        let indexTable =                    p.TryGetResult IndexTable |> Option.orElseWith  (fun () -> c.DynamoIndexTable) |> Option.defaultWith (fun () -> table + indexSuffix)
        let fromTail =                      p.Contains FromTail
        let tailSleepInterval =             TimeSpan.FromMilliseconds 500.
        let batchSizeCutoff =               p.GetResult(MaxItems, 100)
        let client =                        connector.CreateClient()
        let indexStoreClient =              lazy client.ConnectStore("Index", indexTable)
        member _.Connect() =
            connector.LogConfiguration()
            client.ConnectStore("Main", table) |> DynamoStoreContext.create
        member _.MonitoringParams(log : ILogger) =
            log.Information("DynamoStoreSource BatchSizeCutoff {batchSizeCutoff} No event hydration", batchSizeCutoff)
            let indexStoreClient = indexStoreClient.Value
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            indexStoreClient, fromTail, batchSizeCutoff, tailSleepInterval
        member _.CreateCheckpointStore(group, cache) =
            let indexTable = indexStoreClient.Value
            indexTable.CreateCheckpointService(group, cache, Config.log)

module Mdb =
    
    open Args.Configuration.Mdb
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-c">]           ConnectionString of string
        | [<AltCommandLine "-r"; Unique>]   ReadConnectionString of string
        | [<AltCommandLine "-cp">]          CheckpointConnectionString of string
        | [<AltCommandLine "-s">]           Schema of string
        | [<AltCommandLine "-b"; Unique>]   BatchSize of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionString _ ->     $"Connection string for the postgres database housing message-db when writing. (Optional if environment variable {CONNECTION_STRING} is defined)"
                | ReadConnectionString _ -> $"Connection string for the postgres database housing message-db when reading. (Defaults to the (write) Connection String; Optional if environment variable {READ_CONN_STRING} is defined)"
                | CheckpointConnectionString _ -> "Connection string used for the checkpoint store. If not specified, defaults to the connection string argument"
                | Schema _ ->               $"Schema that should contain the checkpoints table Optional if environment variable {SCHEMA} is defined"
                | BatchSize _ ->            "maximum events to load in a batch. Default: 100"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."

    type Arguments(c : Args.Configuration, p : ParseResults<Parameters>) =
        let writeConnStr =                  p.TryGetResult ConnectionString |> Option.defaultWith (fun () -> c.MdbConnectionString)
        let readConnStr =                   p.TryGetResult ReadConnectionString |> Option.orElseWith (fun () -> c.MdbReadConnectionString) |> Option.defaultValue writeConnStr
        let checkpointConnStr =             p.TryGetResult CheckpointConnectionString |> Option.defaultValue writeConnStr
        let schema =                        p.TryGetResult Schema |> Option.defaultWith (fun () -> c.MdbSchema)
        let fromTail =                      p.Contains FromTail
        let batchSize =                     p.GetResult(BatchSize, 100)
        let tailSleepInterval =             TimeSpan.FromMilliseconds 500.
        member _.Connect() =
                                            let connStrWithoutPassword = Npgsql.NpgsqlConnectionStringBuilder(checkpointConnStr, Password = null)
                                            let sanitize s = Npgsql.NpgsqlConnectionStringBuilder(s, Password = null)
                                            Log.Information("Npgsql checkpoint connection {connectionString}", sanitize checkpointConnStr)
                                            if writeConnStr = readConnStr then
                                                Log.Information("MessageDB connection {connectionString}", sanitize writeConnStr)
                                            else
                                                Log.Information("MessageDB write connection {connectionString}", sanitize writeConnStr)
                                                Log.Information("MessageDB read connection {connectionString}", sanitize readConnStr)
                                            let client = Equinox.MessageDb.MessageDbClient(writeConnStr, readConnStr)
                                            Equinox.MessageDb.MessageDbContext(client, batchSize)
        member _.MonitoringParams(log : ILogger) =
            log.Information("MessageDbSource batchSize {batchSize} Checkpoints schema {schema}", batchSize, schema)
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            readConnStr, fromTail, batchSize, tailSleepInterval
        member _.CreateCheckpointStore(group) =
            Propulsion.MessageDb.ReaderCheckpoint.CheckpointStore(checkpointConnStr, schema, group, TimeSpan.FromSeconds 5.)
