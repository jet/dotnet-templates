[<AutoOpen>]
module SyncTemplate.Infrastructure

open Serilog
open Serilog.Events
open System

module Store =

    module Metrics =

        let log = Log.ForContext("isMetric", true)

module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Log =

    /// Allow logging to filter out emission of log messages whose information is also surfaced as metrics
    let isStoreMetrics e = Filters.Matching.WithProperty("isMetric").Invoke e
    /// The Propulsion.Streams.Prometheus LogSink uses this well-known property to identify consumer group associated with the Scheduler
    let forGroup group = Log.ForContext("group", group)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, verbose, verboseStore, ?maybeSeqEndpoint) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose then c.MinimumLevel.Debug() else c
        |> fun c -> let ingesterLevel = if verboseStore then LogEventLevel.Debug else LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Sinks.Factory>.FullName, ingesterLevel)
        |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.CosmosStore.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Internal.Writer.Result>.FullName, generalLevel)
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
                    let configure (a: Configuration.LoggerSinkConfiguration): unit =
                        a.Logger(fun l ->
                            l.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
                             .WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink()) |> ignore) |> ignore
                        a.Logger(fun l ->
                            let isWriterA = Filters.Matching.FromSource<Propulsion.EventStore.Internal.Writer.Result>().Invoke
                            let isWriterB = Filters.Matching.FromSource<Propulsion.CosmosStore.Internal.Writer.Result>().Invoke
                            let l =
                                if verboseStore then l
                                else l.Filter.ByExcluding(fun x -> Log.isStoreMetrics x || isWriterA x || isWriterB x)
                            l.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                            |> ignore) |> ignore
                    c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)
        |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)

module CosmosStoreConnector =

    let private get (role: string) (client: Microsoft.Azure.Cosmos.CosmosClient) databaseId containerId =
        Log.Information("CosmosDB {role} Database {database} Container {container}", role, databaseId, containerId)
        client.GetDatabase(databaseId).GetContainer(containerId)
    let getSource = get "Source"
    let getLeases = get "Leases"
    let getSourceAndLeases client databaseId containerId auxContainerId =
        getSource client databaseId containerId, getLeases client databaseId auxContainerId

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents, ?queryMaxItems, ?tipMaxJsonLength, ?skipLog) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents, ?queryMaxItems = queryMaxItems, ?tipMaxJsonLength = tipMaxJsonLength)
        if skipLog = Some true then () else c.LogConfiguration(role, databaseId, containerId)
        c
        
type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(role, databaseId: string, containers: string[]) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDB {role} {mode} {endpointUri} {db} {containers} timeout {timeout}s Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        role, o.ConnectionMode, x.Endpoint, databaseId, containers, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
    member private x.CreateAndInitialize(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.CreateAndInitialize(databaseId, containers)
    member private x.Connect(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.Connect(databaseId, containers)
    member x.ConnectTarget(role, databaseId, containerId, maxEvents) = async {
        let! client = x.Connect(role, databaseId, [| containerId |])
        return client.CreateContext(role, databaseId, containerId, tipMaxEvents = maxEvents) }
    member x.LeasesContainer(databaseId, auxContainerId) =
        let client = x.CreateUninitialized()
        CosmosStoreConnector.getLeases client databaseId auxContainerId
    member x.ConnectFeed(databaseId, containerId, auxContainerId) = async {
        let! cosmosClient = x.CreateAndInitialize("Source", databaseId, [| containerId; auxContainerId |])
        let source, leases = CosmosStoreConnector.getSourceAndLeases cosmosClient databaseId containerId auxContainerId
        return source, leases }
    member x.ConnectFeedExternalLeases(databaseId, containerId, auxContainer: Microsoft.Azure.Cosmos.Container) = async {
        let! cosmosClient = x.CreateAndInitialize("Source", databaseId, [| containerId |])
        let source = CosmosStoreConnector.getSource cosmosClient databaseId containerId
        let leases = CosmosStoreConnector.getLeases auxContainer.Database.Client auxContainer.Database.Id auxContainer.Id
        return source, leases }
