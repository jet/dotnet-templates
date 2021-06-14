[<AutoOpen>]
module ProjectorTemplate.Infrastructure

open Serilog
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

#if cosmos
[<System.Runtime.CompilerServices.Extension>]
type LoggerConfigurationExtensions() =

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ExcludeChangeFeedProcessorV2InternalDiagnostics(c : LoggerConfiguration) =
        let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
        c.Filter.ByExcluding(fun x -> isCfp x)

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        // LibLog writes to the global logger, so we need to control the emission
        let cfpl = if verbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
        |> fun c -> if verbose then c else c.ExcludeChangeFeedProcessorV2InternalDiagnostics()
#else
#if esdb
module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256 // default is 0
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)
#endif
#endif
type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(connectionName, databaseId, containerId) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDb {name} {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        connectionName, o.ConnectionMode, x.Endpoint, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
        Log.Information("CosmosDb {name} Database {database} Container {container}",
                        connectionName, databaseId, containerId)

    /// Use sparingly; in general one wants to use CreateAndInitialize to avoid slow first requests
    member x.CreateUninitialized(databaseId, containerId) =
        x.CreateUninitialized().GetDatabase(databaseId).GetContainer(containerId)
    
    /// Connect a CosmosStoreClient, including warming up
    member x.ConnectStore(connectionName, databaseId, containerId) =
        x.LogConfiguration(connectionName, databaseId, containerId)
        Equinox.CosmosStore.CosmosStoreClient.Connect(x.CreateAndInitialize, databaseId, containerId)
        
    /// Creates a CosmosClient suitable for running a CFP via CosmosStoreSource
    member x.ConnectMonitored(databaseId, containerId, ?connectionName) =
        x.LogConfiguration(defaultArg connectionName "Source", databaseId, containerId)
        x.CreateUninitialized(databaseId, containerId)

    /// Connects to a Store as both a ChangeFeedProcessor Monitored Container and a CosmosStoreClient
    member x.ConnectStoreAndMonitored(databaseId, containerId) =
        let monitored = x.ConnectMonitored(databaseId, containerId, "Main")
        let storeClient = Equinox.CosmosStore.CosmosStoreClient(monitored.Database.Client, databaseId, containerId)
        storeClient, monitored

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
#if cosmos
    static member Configure(configuration : LoggerConfiguration, ?verbose, ?changeFeedProcessorVerbose) =
#else
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
#endif
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
#if cosmos
        |> fun c -> c.ConfigureChangeFeedProcessorLogging((changeFeedProcessorVerbose = Some true))
#endif
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                    c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
