namespace SyncTemplate

open Serilog
open Serilog.Events
open System

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

[<System.Runtime.CompilerServices.Extension>]
type LoggerConfigurationExtensions() =

    [<System.Runtime.CompilerServices.Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        let cfpl = if verbose then LogEventLevel.Debug else LogEventLevel.Warning
        // TODO figure out what CFP v3 requires
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, verbose, changeFeedProcessorVerbose, ?maybeSeqEndpoint) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose then c.MinimumLevel.Debug() else c
        |> fun c -> c.ConfigureChangeFeedProcessorLogging(changeFeedProcessorVerbose)
        |> fun c -> let ingesterLevel = if changeFeedProcessorVerbose then LogEventLevel.Debug else LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamSchedulingEngine>.FullName, ingesterLevel)
        |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.CosmosStore.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Checkpoint.CheckpointSeries>.FullName, LogEventLevel.Information)
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {NewLine}{Exception}"
                    let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                        a.Logger(fun l ->
                            l.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
                             .WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink()) |> ignore) |> ignore
                        a.Logger(fun l ->
                            let isEqx = Filters.Matching.FromSource<Equinox.CosmosStore.CosmosStoreContext>().Invoke
                            let isWriterA = Filters.Matching.FromSource<Propulsion.EventStore.Internal.Writer.Result>().Invoke
                            let isWriterB = Filters.Matching.FromSource<Propulsion.CosmosStore.Internal.Writer.Result>().Invoke
                            let isCp = Filters.Matching.FromSource<Propulsion.EventStore.Checkpoint.CheckpointSeries>().Invoke
                            let l =
                                if changeFeedProcessorVerbose then l
                                else l.Filter.ByExcluding(fun x -> isEqx x || isWriterA x || isWriterB x || isCp x)
                            l.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                            |> ignore) |> ignore
                    c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)
        |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256 // default is 0
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

[<AutoOpen>]
module ConnectorExtensions =

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

