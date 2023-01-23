[<AutoOpen>]
module Infrastructure.Helpers

open Serilog
open System

module Log =
    
    /// Allow logging to filter out emission of log messages whose information is also surfaced as metrics
    let isStoreMetrics e = Filters.Matching.WithProperty("isMetric").Invoke e
    
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
    
    let parallelLimit dop computations =
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

/// Equinox and Propulsion provide metrics as properties in log emissions
/// These helpers wire those to pass through virtual Log Sinks that expose them as Prometheus metrics.
module Sinks =

    let tags appName = ["app", appName]

    let private equinoxMetricsOnly tags (l : LoggerConfiguration) =
        l.WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
         .WriteTo.Sink(Equinox.DynamoStore.Prometheus.LogSink(tags))
         .WriteTo.Sink(Equinox.MessageDb.Log.InternalMetrics.Stats.LogSink())

    let private equinoxAndPropulsionMetrics tags group (l : LoggerConfiguration) =
        l |> equinoxMetricsOnly tags
          |> fun l -> l.WriteTo.Sink(Propulsion.Prometheus.LogSink(tags, group))

    let equinoxAndPropulsionFeedMetrics tags group (l : LoggerConfiguration) =
        l |> equinoxAndPropulsionMetrics tags group
          |> fun l -> l.WriteTo.Sink(Propulsion.Feed.Prometheus.LogSink(tags))

    let console (configuration : LoggerConfiguration) =
        let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
        configuration.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c

    [<System.Runtime.CompilerServices.Extension>]
    static member private Sinks(configuration : LoggerConfiguration, configureMetricsSinks, configureConsoleSink, ?isMetric) =
        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
            a.Logger(configureMetricsSinks >> ignore) |> ignore // unconditionally feed all log events to the metrics sinks
            a.Logger(fun l -> // but filter what gets emitted to the console sink
                let l = match isMetric with None -> l | Some predicate -> l.Filter.ByExcluding(Func<Serilog.Events.LogEvent, bool> predicate)
                configureConsoleSink l |> ignore)
            |> ignore
        configuration.WriteTo.Async(bufferSize = 65536, blockWhenFull = true, configure = System.Action<_> configure)

    [<System.Runtime.CompilerServices.Extension>]
    static member Sinks(configuration : LoggerConfiguration, configureMetricsSinks, verboseStore) =
        configuration.Sinks(configureMetricsSinks, Sinks.console, ?isMetric = if verboseStore then None else Some Log.isStoreMetrics)
