namespace Infrastructure

open System
open System.Threading.Tasks

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type SourceConfig =
    | Memory of store : Equinox.MemoryStore.VolatileStore<struct (int * ReadOnlyMemory<byte>)>
    | Dynamo of indexStore : Equinox.DynamoStore.DynamoStoreClient
        * checkpoints : Propulsion.Feed.IFeedCheckpointStore
        * loading : Propulsion.DynamoStore.EventLoadMode
        * startFromTail : bool
        * batchSizeCutoff : int
        * tailSleepInterval : TimeSpan
        * statsInterval : TimeSpan
    | Mdb of connectionString : string
        * checkpoints : Propulsion.Feed.IFeedCheckpointStore
        * startFromTail : bool
        * batchSize : int
        * tailSleepInterval : TimeSpan
        * statsInterval : TimeSpan

module SourceConfig =
    module Memory =
        open Propulsion.MemoryStore
        let start log (sink : Propulsion.Streams.Default.Sink) categories
            (store : Equinox.MemoryStore.VolatileStore<_>) : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source = MemoryStoreSource(log, store, (fun x -> Array.contains x categories), sink)
            source.Start(), Some (fun _propagationDelay -> source.Monitor.AwaitCompletion(ignoreSubsequent = false))
    module Dynamo =
        open Propulsion.DynamoStore
        let private create (log, storeLog) (sink : Propulsion.Streams.Default.Sink) categories
            (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) trancheIds =
            DynamoStoreSource(
                log, statsInterval,
                indexStore, batchSizeCutoff, tailSleepInterval,
                checkpoints, sink, loadMode, categories = categories,
                startFromTail = startFromTail, storeLog = storeLog, ?trancheIds = trancheIds)
        let start (log, storeLog) sink categories (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval)
            : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source = create (log, storeLog) sink categories (indexStore, checkpoints, loadMode, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) None
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))
    module Mdb =
        open Propulsion.MessageDb
        let start log sink categories (connectionString, checkpoints, startFromTail, batchSize, tailSleepInterval, statsInterval)
            : Propulsion.Pipeline * (TimeSpan -> Task<unit>) option =
            let source =
                MessageDbSource(
                    log, statsInterval,
                    connectionString, batchSize, tailSleepInterval,
                    checkpoints, sink, categories,
                    startFromTail = startFromTail)
            let source = source.Start()
            source, Some (fun propagationDelay -> source.Monitor.AwaitCompletion(propagationDelay, ignoreSubsequent = false))

    let start (log, storeLog) sink categories : SourceConfig -> Propulsion.Pipeline * (TimeSpan -> Task<unit>) option = function
        | SourceConfig.Memory volatileStore ->
            Memory.start log sink categories volatileStore
        | SourceConfig.Dynamo (indexStore, checkpoints, loading, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval) ->
            Dynamo.start (log, storeLog) sink categories (indexStore, checkpoints, loading, startFromTail, batchSizeCutoff, tailSleepInterval, statsInterval)
        | SourceConfig.Mdb (connectionString, checkpoints, startFromTail, batchSize, tailSleepInterval, statsInterval) ->
            Mdb.start log sink categories (connectionString, checkpoints, startFromTail, batchSize, tailSleepInterval, statsInterval)
