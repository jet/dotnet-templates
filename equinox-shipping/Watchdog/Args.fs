/// Commandline arguments and/or secrets loading specifications
module Args

open System
open FSharp.Control

let [<Literal>] REGION =                    "EQUINOX_DYNAMO_REGION"
let [<Literal>] SERVICE_URL =               "EQUINOX_DYNAMO_SERVICE_URL"
let [<Literal>] ACCESS_KEY =                "EQUINOX_DYNAMO_ACCESS_KEY_ID"
let [<Literal>] SECRET_KEY =                "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
let [<Literal>] TABLE =                     "EQUINOX_DYNAMO_TABLE"
let [<Literal>] INDEX_TABLE =               "EQUINOX_DYNAMO_TABLE_INDEX"

let [<Literal>] CONNECTION =                "EQUINOX_COSMOS_CONNECTION"
let [<Literal>] DATABASE =                  "EQUINOX_COSMOS_DATABASE"
let [<Literal>] CONTAINER =                 "EQUINOX_COSMOS_CONTAINER"

type Configuration(tryGet: string -> string option) =

    let get key =                           match tryGet key with Some value -> value | None -> failwith $"Missing Argument/Environment Variable %s{key}"
    member val tryGet =                     tryGet

    member x.CosmosConnection =             get CONNECTION
    member x.CosmosDatabase =               get DATABASE
    member x.CosmosContainer =              get CONTAINER

    member _.DynamoServiceUrl =             get SERVICE_URL
    member _.DynamoAccessKey =              get ACCESS_KEY
    member _.DynamoSecretKey =              get SECRET_KEY
    member _.DynamoTable =                  get TABLE
    member _.DynamoRegion =                 tryGet REGION

    member _.EventStoreConnection =         get "EQUINOX_ES_CONNECTION"
    member _.MaybeEventStoreConnection =    tryGet "EQUINOX_ES_CONNECTION"
    member _.MaybeEventStoreCredentials =   tryGet "EQUINOX_ES_CREDENTIALS"

    member x.PrometheusPort =               tryGet "PROMETHEUS_PORT" |> Option.map int

open Argu

module Cosmos =

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V"; Unique>]   Verbose
        | [<AltCommandLine "-m">]           ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-s">]           Connection of string
        | [<AltCommandLine "-d">]           Database of string
        | [<AltCommandLine "-c">]           Container of string
        | [<AltCommandLine "-r">]           Retries of int
        | [<AltCommandLine "-rt">]          RetriesWaitTime of float
        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Verbose ->                "request verbose logging."
                | ConnectionMode _ ->       "override the connection mode. Default: Direct."
                | Connection _ ->           $"specify a connection string for a Cosmos account. (optional if environment variable $%s{CONNECTION} specified)"
                | Database _ ->             $"specify a database name for store. (optional if environment variable $%s{DATABASE} specified)"
                | Container _ ->            $"specify a container name for store. (optional if environment variable $%s{CONTAINER} specified)"
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"

    type Arguments(c: Configuration, p: ParseResults<Parameters>) =
        let connection =                    p.GetResult(Connection, fun () -> c.CosmosConnection)
        let discovery =                     Equinox.CosmosStore.Discovery.ConnectionString connection
        let mode =                          p.TryGetResult ConnectionMode
        let retries =                       p.GetResult(Retries, 1)
        let maxRetryWaitTime =              p.GetResult(RetriesWaitTime, 5.) |> TimeSpan.FromSeconds
        let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, retries, maxRetryWaitTime, ?mode = mode)
        let database =                      p.GetResult(Database, fun () -> c.CosmosDatabase)
        let container =                     p.GetResult(Container, fun () -> c.CosmosContainer)
        member val Verbose =                p.Contains Verbose
        member _.Connect() =                connector.ConnectContext(database, container)

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
                | RegionProfile _ ->        $"specify an AWS Region (aka System Name, e.g. \"us-east-1\") to connect to using the implicit AWS SDK/tooling config and/or environment variables etc. Optional if:\n" +
                                            "1) $%s{REGION} specified OR\n" +
                                            "2) Explicit `ServiceUrl`/$" + SERVICE_URL + "+`AccessKey`/$" + ACCESS_KEY + "+`Secret Key`/$" + SECRET_KEY + " specified.\n" +
                                            "See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html for details"
                | ServiceUrl _ ->           "specify a server endpoint for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SERVICE_URL + " specified)"
                | AccessKey _ ->            "specify an access key id for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + ACCESS_KEY + " specified)"
                | SecretKey _ ->            "specify a secret access key for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SECRET_KEY + " specified)"
                | Table _ ->                "specify a table name for the primary store. (optional if $" + TABLE + " specified)"
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesTimeoutS _ ->      "specify max wait-time including retries in seconds (default: 5)"

    type Arguments(c: Configuration, p: ParseResults<Parameters>) =
        let conn =                          match p.TryGetResult RegionProfile |> Option.orElseWith (fun () -> c.DynamoRegion) with
                                            | Some systemName ->
                                                Choice1Of2 systemName
                                            | None ->
                                                let serviceUrl =  p.GetResult(ServiceUrl, fun () -> c.DynamoServiceUrl)
                                                let accessKey =   p.GetResult(AccessKey, fun () -> c.DynamoAccessKey)
                                                let secretKey =   p.GetResult(SecretKey, fun () -> c.DynamoSecretKey)
                                                Choice2Of2 (serviceUrl, accessKey, secretKey)
        let connector =                     let timeout = p.GetResult(RetriesTimeoutS, 5.) |> TimeSpan.FromSeconds
                                            let retries = p.GetResult(Retries, 1)
                                            match conn with
                                            | Choice1Of2 systemName ->
                                                Equinox.DynamoStore.DynamoStoreConnector(systemName, timeout, retries)
                                            | Choice2Of2 (serviceUrl, accessKey, secretKey) ->
                                                Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        let table =                         p.GetResult(Table, fun () -> c.DynamoTable)
        member _.Connect() =                connector.CreateClient().CreateContext("Main", table)

type [<RequireQualifiedAccess; NoComparison; NoEquality>]
    TargetStoreArgs =
    | Cosmos of Cosmos.Arguments
    | Dynamo of Dynamo.Arguments

module TargetStoreArgs =
    
    let connectTarget targetStore cache: Store.Config<_> =
        match targetStore with
        | TargetStoreArgs.Cosmos a ->
            let context = a.Connect() |> Async.RunSynchronously
            Store.Config.Cosmos (context, cache)
        | TargetStoreArgs.Dynamo a ->
            let context = a.Connect()
            Store.Config.Dynamo (context, cache)
