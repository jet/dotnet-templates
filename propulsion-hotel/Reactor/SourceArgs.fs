module Reactor.SourceArgs

open Argu
open Infrastructure // Args etc
open Serilog
open System

module Store = Domain.Store

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
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."

    type Arguments(c: Args.Configuration, p: ParseResults<Parameters>) =
        let conn =                          match p.TryGetResult RegionProfile |> Option.orElseWith (fun () -> c.DynamoRegion) with
                                            | Some systemName ->
                                                Choice1Of2 systemName
                                            | None ->
                                                let serviceUrl =  p.GetResult(ServiceUrl, fun () -> c.DynamoServiceUrl)
                                                let accessKey =   p.GetResult(AccessKey, fun () -> c.DynamoAccessKey)
                                                let secretKey =   p.GetResult(SecretKey, fun () -> c.DynamoSecretKey)
                                                Choice2Of2 (serviceUrl, accessKey, secretKey)
        let connector =                     let timeout = p.GetResult(RetriesTimeoutS, 60.) |> TimeSpan.FromSeconds
                                            let retries = p.GetResult(Retries, 9)
                                            match conn with
                                            | Choice1Of2 systemName ->
                                                Equinox.DynamoStore.DynamoStoreConnector(systemName, timeout, retries)
                                            | Choice2Of2 (serviceUrl, accessKey, secretKey) ->
                                                Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let table =                         p.GetResult(Table, fun () -> c.DynamoTable)
        let indexSuffix =                   p.GetResult(IndexSuffix, "-index")
        let indexTable =                    p.GetResult(IndexTable, fun () -> defaultArg c.DynamoIndexTable (table + indexSuffix)) 
        let client =                        lazy connector.CreateClient()
        let indexContext =                  lazy client.Value.CreateContext("Index", indexTable)
        let fromTail =                      p.Contains FromTail
        let tailSleepInterval =             TimeSpan.FromMilliseconds 500.
        let batchSizeCutoff =               p.GetResult(MaxItems, 100)
        member _.Connect() =                client.Value.CreateContext("Main", table)
        member _.MonitoringParams(log: ILogger) =
            log.Information("DynamoStoreSource BatchSizeCutoff {batchSizeCutoff} No event hydration", batchSizeCutoff)
            let indexContext = indexContext.Value
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            indexContext, fromTail, batchSizeCutoff, tailSleepInterval
        member _.CreateCheckpointStore(group, cache) =
            indexContext.Value.CreateCheckpointService(group, cache, Store.Metrics.log)

module Mdb =
    
    open Args.Configuration.Mdb
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-c">]           ConnectionString of string
        | [<AltCommandLine "-r"; Unique>]   ReadConnectionString of string
        | [<AltCommandLine "-cp">]          CheckpointConnectionString of string
        | [<AltCommandLine "-cs">]          CheckpointSchema of string
        | [<AltCommandLine "-b"; Unique>]   BatchSize of int
        | [<AltCommandLine "-t"; Unique>]   TailSleepIntervalMs of int
        | [<AltCommandLine "-Z"; Unique>]   FromTail
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionString _ ->     $"Connection string for the postgres database housing message-db when writing. (Optional if environment variable {CONNECTION_STRING} is defined)"
                | ReadConnectionString _ -> $"Connection string for the postgres database housing message-db when reading. (Defaults to the (write) Connection String; Optional if environment variable {READ_CONN_STRING} is defined)"
                | CheckpointConnectionString _ -> "Connection string used for the checkpoint store. If not specified, defaults to the connection string argument"
                | CheckpointSchema _ ->     $"Schema that should contain the checkpoints table. Optional if environment variable {SCHEMA} is defined"
                | BatchSize _ ->            "maximum events to load in a batch. Default: 1000"
                | TailSleepIntervalMs _ ->  "How long to sleep in ms once the consumer has hit the tail (default: 100ms)"
                | FromTail ->               "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
    type Arguments(c: Args.Configuration, p: ParseResults<Parameters>) =
        let writeConnStr =                  p.GetResult(ConnectionString, fun () -> c.MdbConnectionString)
        let readConnStr =                   p.TryGetResult ReadConnectionString |> Option.orElseWith (fun () -> c.MdbReadConnectionString) |> Option.defaultValue writeConnStr
        let checkpointConnStr =             p.GetResult(CheckpointConnectionString, writeConnStr)
        let schema =                        p.GetResult(CheckpointSchema, fun () -> c.MdbSchema)
        let fromTail =                      p.Contains FromTail
        let batchSize =                     p.GetResult(BatchSize, 1000)
        let tailSleepInterval =             p.GetResult(TailSleepIntervalMs, 100) |> TimeSpan.FromMilliseconds
        member _.Connect() =
                                            let sanitize (cs: string) = Npgsql.NpgsqlConnectionStringBuilder(cs, Password = null)
                                            Log.Information("Npgsql checkpoint connection {connectionString}", sanitize checkpointConnStr)
                                            if writeConnStr = readConnStr then
                                                Log.Information("MessageDB connection {connectionString}", sanitize writeConnStr)
                                            else
                                                Log.Information("MessageDB write connection {connectionString}", sanitize writeConnStr)
                                                Log.Information("MessageDB read connection {connectionString}", sanitize readConnStr)
                                            let client = Equinox.MessageDb.MessageDbClient(writeConnStr, readConnStr)
                                            Equinox.MessageDb.MessageDbContext(client, batchSize)
        member _.MonitoringParams(log: ILogger) =
            log.Information("MessageDbSource batchSize {batchSize} Checkpoints schema {schema}", batchSize, schema)
            if fromTail then log.Warning("(If new projector group) Skipping projection of all existing events.")
            readConnStr, fromTail, batchSize, tailSleepInterval
        member _.CreateCheckpointStore(group) =
            Propulsion.MessageDb.ReaderCheckpoint.CheckpointStore(checkpointConnStr, schema, group)
