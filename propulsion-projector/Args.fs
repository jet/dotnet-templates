/// Commandline arguments and/or secrets loading specifications
module ProjectorTemplate.Args

open System

exception MissingArg of message : string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

let [<Literal>] REGION =                    "EQUINOX_DYNAMO_REGION"
let [<Literal>] SERVICE_URL =               "EQUINOX_DYNAMO_SERVICE_URL"
let [<Literal>] ACCESS_KEY =                "EQUINOX_DYNAMO_ACCESS_KEY_ID"
let [<Literal>] SECRET_KEY =                "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
let [<Literal>] TABLE =                     "EQUINOX_DYNAMO_TABLE"
let [<Literal>] INDEX_TABLE =               "EQUINOX_DYNAMO_TABLE_INDEX"

type Configuration(tryGet : string -> string option) =

    member val tryGet =                     tryGet
    member _.get key =                      match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"

    member x.CosmosConnection =             x.get "EQUINOX_COSMOS_CONNECTION"
    member x.CosmosDatabase =               x.get "EQUINOX_COSMOS_DATABASE"
    member x.CosmosContainer =              x.get "EQUINOX_COSMOS_CONTAINER"

    member x.DynamoServiceUrl =             x.get SERVICE_URL
    member x.DynamoAccessKey =              x.get ACCESS_KEY
    member x.DynamoSecretKey =              x.get SECRET_KEY
    member x.DynamoTable =                  x.get TABLE
    member x.DynamoRegion =                 x.tryGet REGION

    member x.EventStoreConnection =         x.get "EQUINOX_ES_CONNECTION"
    // member x.EventStoreCredentials =        x.get "EQUINOX_ES_CREDENTIALS"
    member _.MaybeEventStoreConnection =    tryGet "EQUINOX_ES_CONNECTION"
    member _.MaybeEventStoreCredentials =   tryGet "EQUINOX_ES_CREDENTIALS"

    member x.SqlStreamStoreConnection =     x.get "SQLSTREAMSTORE_CONNECTION"
    member x.SqlStreamStoreCredentials =    tryGet "SQLSTREAMSTORE_CREDENTIALS"
    member x.SqlStreamStoreCredentialsCheckpoints = tryGet "SQLSTREAMSTORE_CREDENTIALS_CHECKPOINTS"
    member x.SqlStreamStoreDatabase =       x.get "SQLSTREAMSTORE_DATABASE"
    member x.SqlStreamStoreContainer =      x.get "SQLSTREAMSTORE_CONTAINER"

//#if kafka
    member x.Broker =                       x.get "PROPULSION_KAFKA_BROKER"
    member x.Topic =                        x.get "PROPULSION_KAFKA_TOPIC"
//#endif
    
    member x.PrometheusPort =               tryGet "PROMETHEUS_PORT" |> Option.map int

// Type used to represent where checkpoints (for either the FeedConsumer position, or for a Reactor's Event Store subscription position) will be stored
// In a typical app you don't have anything like this as you'll simply use your primary Event Store (see)
module Checkpoints =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type Store =
        | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
        | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
        (*  Propulsion.EventStoreDb does not implement a native checkpoint storage mechanism,
            perhaps port https://github.com/absolutejam/Propulsion.EventStoreDB ?
            or fork/finish https://github.com/jet/dotnet-templates/pull/81
            alternately one could use a SQL Server DB via Propulsion.SqlStreamStore

            For now, we store the Checkpoints in one of the above stores *)

    let create (consumerGroup, checkpointInterval) storeLog : Store  -> Propulsion.Feed.IFeedCheckpointStore = function
        | Store.Cosmos (context, cache) ->
            Propulsion.Feed.ReaderCheckpoint.CosmosStore.create storeLog (consumerGroup, checkpointInterval) (context, cache)
        | Store.Dynamo (context, cache) ->
            Propulsion.Feed.ReaderCheckpoint.DynamoStore.create storeLog (consumerGroup, checkpointInterval) (context, cache)
    let createCheckpointStore (group, checkpointInterval, store : Config.Store) : Propulsion.Feed.IFeedCheckpointStore =
        let checkpointStore =
            match store with
            | Config.Store.Cosmos (context, cache) -> Store.Cosmos (context, cache)
            | Config.Store.Dynamo (context, cache) -> Store.Dynamo (context, cache)
        create (group, checkpointInterval) Config.log checkpointStore

open Argu

#if kafka
 type [<NoEquality; NoComparison>] KafkaSinkParameters =
    | [<AltCommandLine "-b"; Unique>]   Broker of string
    | [<AltCommandLine "-t"; Unique>]   Topic of string
    interface IArgParserTemplate with
        member p.Usage = p |> function
            | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)"
            | Topic _ ->                "specify Kafka Topic Id. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)"
            
type KafkaSinkArguments(c : Configuration, p : ParseResults<KafkaSinkParameters>) =
    member val Broker =                 p.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
    member val Topic =                  p.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
    member x.BuildTargetParams() =      x.Broker, x.Topic

#endif

#if (esdb || sss || cosmos)
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
#if kafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<KafkaSinkParameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose _ ->              "request verbose logging."
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->             "specify a database name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->            "specify a container name for Cosmos store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | Timeout _ ->              "specify operation timeout in seconds (default: 5)."
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
#if kafka
                | Kafka _ ->                "Kafka Sink parameters."
#endif
    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let connection =                    p.TryGetResult Connection |> Option.defaultWith (fun () -> c.CosmosConnection)
        let discovery =                     Equinox.CosmosStore.Discovery.ConnectionString connection
        let mode =                          p.TryGetResult ConnectionMode
        let timeout =                       p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.TryGetResult Database |> Option.defaultWith (fun () -> c.CosmosDatabase)
        let container =                     p.TryGetResult Container |> Option.defaultWith (fun () -> c.CosmosContainer)
        member val Verbose =                p.Contains Verbose
        member _.Connect() =                connector.ConnectStore("Target", database, container)
#if kafka
        member val Kafka =
            match p.GetSubCommand() with
            | Parameters.Kafka kafka -> KafkaSinkArguments(c, kafka)
            | _ -> missingArg "Must specify `kafka` arguments"
#endif

#endif // cosmos
#if (esdb || sss || dynamo)
module Dynamo =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]           Verbose
        | [<AltCommandLine "-sr">]          RegionProfile of string
        | [<AltCommandLine "-su">]          ServiceUrl of string
        | [<AltCommandLine "-sa">]          AccessKey of string
        | [<AltCommandLine "-ss">]          SecretKey of string
        | [<AltCommandLine "-t">]           Table of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesTimeoutS of float
#if kafka
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Kafka of ParseResults<KafkaSinkParameters>
#endif
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "Include low level Store logging."
                | RegionProfile _ ->        "specify an AWS Region (aka System Name, e.g. \"us-east-1\") to connect to using the implicit AWS SDK/tooling config and/or environment variables etc. Optional if:\n" +
                                            "1) $" + REGION + " specified OR\n" +
                                            "2) Explicit `ServiceUrl`/$" + SERVICE_URL + "+`AccessKey`/$" + ACCESS_KEY + "+`Secret Key`/$" + SECRET_KEY + " specified.\n" +
                                            "See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html for details"
                | ServiceUrl _ ->           "specify a server endpoint for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SERVICE_URL + " specified)"
                | AccessKey _ ->            "specify an access key id for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + ACCESS_KEY + " specified)"
                | SecretKey _ ->            "specify a secret access key for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SECRET_KEY + " specified)"
                | Table _ ->                "specify a table name for the primary store. (optional if $" + TABLE + " specified)"
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesTimeoutS _ ->      "specify max wait-time including retries in seconds (default: 5)"
#if kafka
                | Kafka _ ->                "Kafka Sink parameters."
#endif
    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        let conn =                          match p.TryGetResult RegionProfile |> Option.orElseWith (fun () -> c.DynamoRegion) with
                                            | Some systemName ->
                                                Choice1Of2 systemName
                                            | None ->
                                                let serviceUrl =  p.TryGetResult ServiceUrl |> Option.defaultWith (fun () -> c.DynamoServiceUrl)
                                                let accessKey =   p.TryGetResult AccessKey  |> Option.defaultWith (fun () -> c.DynamoAccessKey)
                                                let secretKey =   p.TryGetResult SecretKey  |> Option.defaultWith (fun () -> c.DynamoSecretKey)
                                                Choice2Of2 (serviceUrl, accessKey, secretKey)
        let retries =                       p.GetResult(Retries, 1)
        let timeout =                       p.GetResult(RetriesTimeoutS, 5.) |> TimeSpan.FromSeconds
        let connector =                     match conn with
                                            | Choice1Of2 systemName ->
                                                Equinox.DynamoStore.DynamoStoreConnector(systemName, timeout, retries)
                                            | Choice2Of2 (serviceUrl, accessKey, secretKey) ->
                                                Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let table =                         p.TryGetResult Table      |> Option.defaultWith (fun () -> c.DynamoTable)
        member val Verbose =                p.Contains Verbose
        member _.Connect() =                connector.LogConfiguration()
                                            let client = connector.CreateClient()
                                            client.ConnectStore("Main", table)
#if kafka
        member val Kafka =
            match p.GetSubCommand() with
            | Kafka kafka -> KafkaSinkArguments(c, kafka)
            | _ -> missingArg "Must specify `kafka` arguments"
#endif

#endif // dynamo

type [<RequireQualifiedAccess; NoComparison; NoEquality>]
    TargetStoreArgs =
    | Cosmos of Cosmos.Arguments
    | Dynamo of Dynamo.Arguments

module TargetStoreArgs =
    
    let connectTarget targetStore cache: Config.Store =
        match targetStore with
        | TargetStoreArgs.Cosmos a ->
            let context = a.Connect() |> Async.RunSynchronously |> CosmosStoreContext.create
            Config.Store.Cosmos (context, cache)
        | TargetStoreArgs.Dynamo a ->
            let context = a.Connect() |> DynamoStoreContext.create
            Config.Store.Dynamo (context, cache)
