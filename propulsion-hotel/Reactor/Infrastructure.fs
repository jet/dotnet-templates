[<AutoOpen>]
module Infrastructure.Helpers

open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Choice =

    let partition f xs =
        let c1, c2 = ResizeArray(), ResizeArray()
        for x in xs do
            match f x with
            | Choice1Of2 r -> c1.Add r
            | Choice2Of2 r -> c2.Add r
        c1.ToArray(), c2.ToArray()

module Async =
    
    let parallelThrottled dop computations =
        Async.Parallel(computations, maxDegreeOfParallelism = dop)
    
type Equinox.DynamoStore.DynamoStoreConnector with

    member x.LogConfiguration() =
        Log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}",
                        x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)

type Equinox.DynamoStore.DynamoStoreClient with

    member internal x.LogConfiguration(role, ?log) =
        (defaultArg log Log.Logger).Information("DynamoStore {role:l} Table {table} Archive {archive}", role, x.TableName, Option.toObj x.ArchiveTableName)
    member client.CreateCheckpointService(consumerGroupName, cache, log, ?checkpointInterval) =
        let checkpointInterval = defaultArg checkpointInterval (TimeSpan.FromHours 1.)
        let context = Equinox.DynamoStore.DynamoStoreContext(client)
        Propulsion.Feed.ReaderCheckpoint.DynamoStore.create log (consumerGroupName, checkpointInterval) (context, cache)

type Equinox.DynamoStore.DynamoStoreContext with

    member internal x.LogConfiguration(log : ILogger) =
        log.Information("DynamoStore Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query Paging {queryMaxItems} items",
                        x.TipOptions.MaxBytes, Option.toNullable x.TipOptions.MaxEvents, x.QueryOptions.MaxItems)

type Amazon.DynamoDBv2.IAmazonDynamoDB with

    member x.ConnectStore(role, table) =
        let storeClient = Equinox.DynamoStore.DynamoStoreClient(x, table)
        storeClient.LogConfiguration(role)
        storeClient

module DynamoStoreContext =

    /// Create with default packing and querying policies. Search for other `module DynamoStoreContext` impls for custom variations
    let create (storeClient : Equinox.DynamoStore.DynamoStoreClient) =
        Equinox.DynamoStore.DynamoStoreContext(storeClient, queryMaxItems = 100)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let t = "{Timestamp:HH:mm:ss} {Level:u1} {Message:lj} {SourceContext} {Properties}{NewLine}{Exception}"
                    c.WriteTo.Console(theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate = t)
