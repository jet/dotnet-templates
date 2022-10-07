namespace Shipping.Watchdog.Lambda

open Amazon.Lambda.Core
open Amazon.Lambda.SQSEvents
open Equinox.DynamoStore
open Serilog
open Shipping.Domain
open Shipping.Infrastructure
open Shipping.Watchdog
open System

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>] ()

type Configuration(appName, ?tryGet) =
    let envVarTryGet = Environment.GetEnvironmentVariable >> Option.ofObj
    let tryGet = defaultArg tryGet envVarTryGet
    let get key = match tryGet key with Some value -> value | None -> failwithf "Missing Argument/Environment Variable %s" key

    member _.DynamoRegion =             tryGet Propulsion.DynamoStore.Lambda.Args.Dynamo.REGION
    member _.DynamoServiceUrl =         get Propulsion.DynamoStore.Lambda.Args.Dynamo.SERVICE_URL
    member _.DynamoAccessKey =          get Propulsion.DynamoStore.Lambda.Args.Dynamo.ACCESS_KEY
    member _.DynamoSecretKey =          get Propulsion.DynamoStore.Lambda.Args.Dynamo.SECRET_KEY
    member _.DynamoTable =              get Propulsion.DynamoStore.Lambda.Args.Dynamo.TABLE
    member _.DynamoIndexTable =         get Propulsion.DynamoStore.Lambda.Args.Dynamo.INDEX_TABLE
    member val ConsumerGroupName =      appName
    member val CacheName =              appName

type Store internal (connector : DynamoStoreConnector, table, indexTable, cacheName, consumerGroupName) =
    let dynamo =                        connector.CreateClient()
    let indexClient =                   DynamoStoreClient(dynamo, indexTable)
    let client =                        DynamoStoreClient(dynamo, table)
    let context =                       DynamoStoreContext(client)
    let cache =                         Equinox.Cache(cacheName, sizeMb = 1)
    let checkpoints =                   indexClient.CreateCheckpointService(consumerGroupName, cache, Config.log)

    new (c : Configuration, requestTimeout, retries) =
        let conn =
            match c.DynamoRegion with
            | Some r -> DynamoStoreConnector(r, requestTimeout, retries)
            | None -> DynamoStoreConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, requestTimeout, retries)
        Store(conn, c.DynamoTable, c.DynamoIndexTable, c.CacheName, c.ConsumerGroupName)

    member val Config =                 Config.Store.Dynamo (context, cache)
    member val DumpMetrics =            Equinox.DynamoStore.Core.Log.InternalMetrics.dump
    member x.CreateSource(trancheIds, sink) =
        let batchSizeCutoff =           100
        let fromTail =                  false
        let tailSleep =                 TimeSpan.FromMilliseconds 500.
        let statsInterval =             TimeSpan.FromMinutes 1.
        let streamsDop =                2
        let loadMode =                  DynamoLoadModeConfig.Hydrate (context, streamsDop)
        Handler.Config.CreateDynamoSource(Log.Logger, sink, (indexClient, checkpoints, loadMode, fromTail, tailSleep, batchSizeCutoff, statsInterval), trancheIds)

/// Wiring for Source and Sink running the Watchdog.Handler
type App(store : Store) =
    
    let stats = Handler.Stats(Log.Logger, TimeSpan.FromMinutes 1., TimeSpan.FromMinutes 2., verboseStore = false, logExternalStats = store.DumpMetrics)
    let processingTimeout = 10. |> TimeSpan.FromSeconds
    let sink =
        let manager =
            let processManagerMaxDop = 4
            FinalizationProcess.Config.create processManagerMaxDop store.Config
        let maxReadAhead = 2
        let maxConcurrentStreams = 8
        // On paper, a 1m window should be fine, give the timeout for a single lifecycle
        // We use a higher value to reduce redundant work in the (edge) case of multiple deliveries due to rate limiting of readers
        let purgeInterval = TimeSpan.FromMinutes 5.
        Handler.Config.StartSink(Log.Logger, stats, manager, processingTimeout, maxReadAhead, maxConcurrentStreams,
                                 wakeForResults = true, purgeInterval = purgeInterval)
        
    member x.RunUntilCaughtUp(tranches, lambdaTimeout) =
        let source = store.CreateSource(tranches, sink)
        let lambdaCutoffDuration = lambdaTimeout - processingTimeout - TimeSpan.FromSeconds 5
        source.RunUntilCaughtUp(lambdaCutoffDuration, stats.StatsInterval)
        
type Function() =

    do  // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we emit them to the log instead)
        let removeMetrics (e : Serilog.Events.LogEvent) =
            e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
            e.RemovePropertyIfPresent(Propulsion.Streams.Log.PropertyTag)
            e.RemovePropertyIfPresent(Propulsion.Feed.Core.Log.PropertyTag)
            e.RemovePropertyIfPresent "isMetric"        
        Log.Logger <- LoggerConfiguration()
            .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink()) // get them counted so we can dump at end of Handle
            .WriteTo.Logger(fun l ->
                l.Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = removeMetrics evt })
                 .WriteTo.Console(outputTemplate = "{Level:u1} {Message:lj} {Properties:j}{NewLine}{Exception}") |> ignore)
            .CreateLogger()
    let config = Configuration("Watchdog.Lambda")
    let store = Store(config, requestTimeout = TimeSpan.FromSeconds 120., retries = 10)
    let app = App(store)

    /// Process for all tranches in the input batch; requeue any triggers that we've not yet fully completed the processing for
    member _.Handle(event : SQSEvent, context : ILambdaContext) : System.Threading.Tasks.Task<SQSBatchResponse> = task {
        let req = Propulsion.DynamoStore.Lambda.SqsNotificationBatch.parse event
        let! updated = app.RunUntilCaughtUp(req.Tranches, context.RemainingTime)
        return Propulsion.DynamoStore.Lambda.SqsNotificationBatch.batchResponseWithFailuresForPositionsNotReached req updated }
