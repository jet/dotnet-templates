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

#if (esdb || sss || dynamo)
// Type used to represent where checkpoints (for either the FeedConsumer position, or for a Reactor's Event Store subscription position) will be stored
// In a typical app you don't have anything like this as you'll simply use your primary Event Store (see)
module Checkpoints =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type Store =
#if (!dynamo)
        | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.Core.ICache
#endif           
        | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache
        (*  Propulsion.EventStoreDb does not implement a native checkpoint storage mechanism,
            perhaps port https://github.com/absolutejam/Propulsion.EventStoreDB ?
            or fork/finish https://github.com/jet/dotnet-templates/pull/81
            alternately one could use a SQL Server DB via Propulsion.SqlStreamStore

            For now, we store the Checkpoints in one of the above stores *)

    let create (consumerGroup, checkpointInterval) storeLog : Store  -> Propulsion.Feed.IFeedCheckpointStore = function
#if (!dynamo)
        | Store.Cosmos (context, cache) ->
            Propulsion.Feed.ReaderCheckpoint.CosmosStore.create storeLog (consumerGroup, checkpointInterval) (context, cache)
#endif            
        | Store.Dynamo (context, cache) ->
            Propulsion.Feed.ReaderCheckpoint.DynamoStore.create storeLog (consumerGroup, checkpointInterval) (context, cache)
    let createCheckpointStore (group, checkpointInterval, store : Config.Store) : Propulsion.Feed.IFeedCheckpointStore =
        let checkpointStore =
            match store with
#if (!dynamo)
            | Config.Store.Cosmos (context, cache) -> Store.Cosmos (context, cache)
#endif           
            | Config.Store.Dynamo (context, cache) -> Store.Dynamo (context, cache)
        create (group, checkpointInterval) Config.log checkpointStore

#endif
open Argu

//#if (esdb || sss || cosmos)
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

//#endif // cosmos
//#if (esdb || sss || dynamo)
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

//#endif // dynamo
