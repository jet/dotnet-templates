namespace Shipping.Watchdog.Lambda

open Amazon.Lambda.Core
open Amazon.Lambda.SQSEvents
open Equinox.DynamoStore
open Propulsion.Feed
open Propulsion.Internal
open Serilog
open Shipping.Domain
open Shipping.Infrastructure
open Shipping.Watchdog
open System
open System.Collections.Generic
open System.Threading.Tasks

[<assembly: LambdaSerializer(typeof<Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer>)>] ()

module App =

    let [<Literal>] Name = "Watchdog.Lambda"
    
type Configuration(?tryGet) =
    let envVarTryGet = Environment.GetEnvironmentVariable >> Option.ofObj
    let tryGet = defaultArg tryGet envVarTryGet
    let get key = match tryGet key with Some value -> value | None -> failwithf "Missing Argument/Environment Variable %s" key

    let [<Literal>] SYSTEM_NAME =       "EQUINOX_DYNAMO_SYSTEM_NAME"
    let [<Literal>] SERVICE_URL =       "EQUINOX_DYNAMO_SERVICE_URL"
    let [<Literal>] ACCESS_KEY =        "EQUINOX_DYNAMO_ACCESS_KEY_ID"
    let [<Literal>] SECRET_KEY =        "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
    let [<Literal>] TABLE =             "EQUINOX_DYNAMO_TABLE"
    let [<Literal>] TABLE_INDEX =       "EQUINOX_DYNAMO_TABLE_INDEX"

    member _.DynamoSystemName =         tryGet SYSTEM_NAME
    member _.DynamoServiceUrl =         get SERVICE_URL
    member _.DynamoAccessKey =          get ACCESS_KEY
    member _.DynamoSecretKey =          get SECRET_KEY
    member _.DynamoTable =              get TABLE
    member _.DynamoIndexTable =         get TABLE_INDEX

type Store internal (connector : DynamoStoreConnector, table, indexTable) =
    let dynamo =                        connector.CreateClient()
    let indexClient =                   DynamoStoreClient(dynamo, indexTable)
    let client =                        DynamoStoreClient(dynamo, table)
    let context =                       DynamoStoreContext(client)
    let cache =                         Equinox.Cache(App.Name, sizeMb = 1)
    let checkpoints =                   let consumerGroupName = App.Name
                                        indexClient.CreateCheckpointService(consumerGroupName, cache, Config.log)

    new (c : Configuration, requestTimeout, retries) =
        let conn =
            match c.DynamoSystemName with
            | Some r -> DynamoStoreConnector(r, requestTimeout, retries)
            | None -> DynamoStoreConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, requestTimeout, retries)
        Store(conn, c.DynamoTable, c.DynamoIndexTable)

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
    let awaitStatsOutput () =
        // The processing loops run on 1s timers, so we busy-wait until they wake
        let timeout = IntervalTimer(TimeSpan.FromSeconds 2)
        while stats.StatsInterval.IsTriggered && not timeout.IsDue do
            System.Threading.Thread.Sleep 1
    let manager =
        let processManagerMaxDop = 4
        FinalizationProcess.Config.create processManagerMaxDop store.Config
    let processingTimeout = 10. |> TimeSpan.FromSeconds
    // On paper, a 1m window should be fine, give the timeout for a single lifecycle
    // We use a higher value to reduce redundant work in the (edge) case of multiple deliveries due to rate limiting of readers
    let purgeInterval = TimeSpan.FromMinutes 5.
    let sink =
        let maxReadAhead = 2
        let maxConcurrentStreams = 8
        Handler.Config.StartSink(Log.Logger, stats, manager, processingTimeout, maxReadAhead, maxConcurrentStreams,
                                 wakeForResults = true, purgeInterval = purgeInterval)
        
    member x.Handle(tranches, lambdaTimeout) = task {
        let sw = Stopwatch.start ()
        
        let src = store.CreateSource(tranches, sink)
        // Kick off reading from the source (Disposal will Stop it if we're exiting due to a timeout; we'll spin up a fresh one when re-triggered)
        use source = src.Start()
        
        // In the case of sustained activity and/or catch-up scenarios, proactively trigger an orderly shutdown of the Source
        // in advance of the Lambda being killed (no point starting new work or incurring DynamoDB CU consumption that won't finish)
        let lambdaCutoffDuration = lambdaTimeout - processingTimeout - TimeSpan.FromSeconds 5
        Task.Delay(lambdaCutoffDuration).ContinueWith(fun _ -> source.Stop()) |> ignore

        // If for some reason we're not provisioned well enough to read something within 1m, no point for paying for a full lambda timeout
        let initialReaderTimeout = TimeSpan.FromMinutes 1.
        do! source.Monitor.AwaitCompletion(initialReaderTimeout, awaitFullyCaughtUp = true, logInterval = TimeSpan.FromSeconds 30)
        // Shut down all processing (we create a fresh Source per Lambda invocation)
        source.Stop()
        
        let isStatsWorthy = sw.ElapsedSeconds > 10
        try if isStatsWorthy then stats.StatsInterval.Trigger()
            // force a final attempt to flush anything not already checkpointed (normally checkpointing is at 5s intervals)
            return! src.Checkpoint()
        finally if isStatsWorthy then awaitStatsOutput () }

/// Each queued Notifier message conveyed to the Lambda represents a Target Position on an Index Tranche
type RequestBatch(event : SQSEvent) =
    let inputs = [|
        for r in event.Records ->
            let trancheId = r.MessageAttributes["TrancheId"].StringValue |> TrancheId.parse
            let position = r.MessageAttributes["Position"].StringValue |> int64 |> Position.parse
            struct (trancheId, position, r.MessageId) |]

    member val Count = inputs.Length
    /// Yields the set of Index Tranches on which we are anticipating there to be work available
    member val Tranches = seq { for trancheId, _, _ in inputs -> trancheId } |> Seq.distinct |> Seq.toArray
    /// Correlates the achieved Tranche Positions with those that triggered the work; requeue any not yet acknowledged as processed
    member _.FailuresForPositionsNotReached(updated : IReadOnlyDictionary<_, _>) =
        let res = SQSBatchResponse()
        let incomplete = ResizeArray()
        for trancheId, pos, messageId in inputs do
            match updated.TryGetValue trancheId with
            | true, pos' when pos' >= pos -> ()
            | _ ->
                res.BatchItemFailures.Add(SQSBatchResponse.BatchItemFailure(ItemIdentifier = messageId))
                incomplete.Add(struct (trancheId, pos))
        struct (res, incomplete.ToArray())

type Function() =

    let removeMetrics (e : Serilog.Events.LogEvent) =
        e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
        e.RemovePropertyIfPresent(Propulsion.Streams.Log.PropertyTag)
        e.RemovePropertyIfPresent(Propulsion.Feed.Core.Log.PropertyTag)
        // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we emit them to the log instead)
        e.RemovePropertyIfPresent "isMetric"
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    do Log.Logger <- LoggerConfiguration()
        .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
        .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = removeMetrics evt })
        .WriteTo.Console(outputTemplate = template)
        .CreateLogger()

    let config = Configuration()
    let store = Store(config, requestTimeout = TimeSpan.FromSeconds 120., retries = 10)
    let app = App(store)

    /// Process for all tranches in the input batch; requeue any triggers that we've not yet fully completed the processing for
    member _.Handle(event : SQSEvent, context : ILambdaContext) : Task<SQSBatchResponse> = task {
        try let req = RequestBatch(event)
            Log.Information("Batch {count} notifications, {tranches} tranches", req.Count, req.Tranches.Length)
            let! updated = app.Handle(req.Tranches, context.RemainingTime)
            let struct (res, requeued) = req.FailuresForPositionsNotReached(updated)
            if Array.any requeued then Log.Information("Batch requeued {requeued}", requeued)
            return res
        finally Equinox.DynamoStore.Core.Log.InternalMetrics.dump Log.Logger }
