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
    let manager =
        let processManagerMaxDop = 4
        FinalizationProcess.Config.create processManagerMaxDop store.Config
    let processingTimeout = 10. |> TimeSpan.FromSeconds
    let sink =
        let maxReadAhead = 2
        let maxConcurrentStreams = 8
        Handler.Config.StartSink(Log.Logger, stats, manager, processingTimeout, maxReadAhead, maxConcurrentStreams,
                                 wakeForResults = true, purgeInterval = stats.StateInterval.Period)
    let initialReaderTimeout = TimeSpan.FromSeconds 3.
    let lambdaTimeout = TimeSpan.FromMinutes 12.
    let lambdaCutoffDuration = lambdaTimeout - initialReaderTimeout - processingTimeout
    
    member x.Handle(tranches) = task {
        let source = store.CreateSource(tranches, sink)
        
        // Kick off reading from the source
        let pipeline = source.Start()
        
        // In the case of sustained activity, we want to proactively trigger an orderly shutdown of the Sink in advance of the Lambda being killed
        Task.Delay(lambdaCutoffDuration).ContinueWith(fun _ -> pipeline.Stop(); stats.StatsInterval.Trigger()) |> ignore

        // wait for processing to stop (we'll continually process as long as there's work available)
        do! source.Monitor.AwaitCompletion(initialReaderTimeout)

        // force a final attempt to flush anything not checkpointed (normally checkpointing is at 5s intervals)
        return! source.Checkpoint() }

/// Each queued Notifier message conveyed to the Lambda represents a Target Position on an Index Tranche
type RequestBatch(event : SQSEvent) =
    let inputs = [|
        for r in event.Records ->
            let trancheId = r.MessageAttributes["TrancheId"].StringValue |> TrancheId.parse
            let position = r.MessageAttributes["Pos"].StringValue |> int64 |> Position.parse
            struct (trancheId, position, r.MessageId) |]

    /// Yields the set of Index Tranches on which we are anticipating there to be work available
    member val Tranches = seq { for trancheId, _, _ in inputs -> trancheId } |> Seq.distinct |> Seq.toArray
    /// Correlates the achieved Tranche Positions with those that triggered the work; requeue any not yet acknowledged as processed
    member _.FailuresForPositionsNotReached(updated : IReadOnlyDictionary<_, _>) =
        let res = SQSBatchResponse()
        for trancheId, pos, messageId in inputs do
            match updated.TryGetValue trancheId with
            | true, pos' when pos' >= pos -> ()
            | _ -> res.BatchItemFailures.Add(SQSBatchResponse.BatchItemFailure(ItemIdentifier = messageId))
        res

type Function() =

    let config = Configuration()
    let store = Store(config, requestTimeout = TimeSpan.FromSeconds 120., retries = 10)
    // TOCONSIDER surface metrics from write activities to prometheus by wiring up Metrics Sink (for now we emit them to the log instead)
    let removeMetrics (e : Serilog.Events.LogEvent) = e.RemovePropertyIfPresent(Equinox.DynamoStore.Core.Log.PropertyTag)
    let template = "{Level:u1} {Message} {Properties}{NewLine}{Exception}"
    do Log.Logger <- LoggerConfiguration()
        .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
        .Enrich.With({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = removeMetrics evt })
        .WriteTo.Console(outputTemplate = template)
        .CreateLogger()
    let app = App(store)

    /// Process for all tranches in the input batch; requeue any triggers that we've not yet fully completed the processing for
    member _.Handle(event : SQSEvent, _context : ILambdaContext) : Task<SQSBatchResponse> = task {
        let req = RequestBatch(event)
        let! updated = app.Handle(req.Tranches)
        return req.FailuresForPositionsNotReached(updated) }
