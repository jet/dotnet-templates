namespace Jet.ConfluentKafka.FSharp

open ProjectorTemplate.Consumer

open Confluent.Kafka
open Newtonsoft.Json.Linq
open Newtonsoft.Json
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

module private Config =
    let validateBrokerUri (u:Uri) =
        if not u.IsAbsoluteUri then invalidArg "broker" "should be of 'host:port' format"
        if String.IsNullOrEmpty u.Authority then 
            // handle a corner case in which Uri instances are erroneously putting the hostname in the `scheme` field.
            if System.Text.RegularExpressions.Regex.IsMatch(string u, "^\S+:[0-9]+$") then string u
            else invalidArg "broker" "should be of 'host:port' format"

        else u.Authority

/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specfic settings
[<NoComparison>]
type KafkaProducerConfig private (inner, broker : Uri) =
    member __.Inner : ProducerConfig = inner
    member __.Broker = broker

    member __.Acks = let v = inner.Acks in v.Value
    member __.MaxInFlight = let v = inner.MaxInFlight in v.Value
    member __.Compression = let v = inner.CompressionType in v.GetValueOrDefault(CompressionType.None)

    /// Creates and wraps a Confluent.Kafka ProducerConfig with the specified settings
    static member Create
        (   clientId : string, broker : Uri, acks,
            /// Message compression. Defaults to None.
            ?compression,
            /// Maximum in-flight requests. Default: 1_000_000.
            /// NB <> 1 implies potential reordering of writes should a batch fail and then succeed in a subsequent retry
            ?maxInFlight,
            /// Time to wait for other items to be produced before sending a batch. Default: 0ms
            /// NB the linger setting alone does provide any hard guarantees; see BatchedProducer.CreateWithConfigOverrides
            ?linger : TimeSpan,
            /// Number of retries. Confluent.Kafka default: 2. Default: 60.
            ?retries,
            /// Backoff interval. Confluent.Kafka default: 100ms. Default: 1s.
            ?retryBackoff,
            /// Statistics Interval. Default: no stats.
            ?statisticsInterval,
            /// Confluent.Kafka default: false. Defaults to true.
            ?socketKeepAlive,
            /// Partition algorithm. Default: `ConsistentRandom`.
            ?partitioner,
            /// Miscellaneous configuration parameters to be passed to the underlying Confluent.Kafka producer configuration.
            ?custom,
            /// Postprocesses the ProducerConfig after the rest of the rules have been applied
            ?customize) =
        let c =
            ProducerConfig(
                ClientId = clientId, BootstrapServers = Config.validateBrokerUri broker,
                RetryBackoffMs = Nullable (match retryBackoff with Some (t : TimeSpan) -> int t.TotalMilliseconds | None -> 1000), // CK default 100ms
                MessageSendMaxRetries = Nullable (defaultArg retries 60), // default 2
                Acks = Nullable acks,
                SocketKeepaliveEnable = Nullable (defaultArg socketKeepAlive true), // default: false
                LogConnectionClose = Nullable false) // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
        maxInFlight |> Option.iter (fun x -> c.MaxInFlight <- Nullable x) // default 1_000_000
        linger |> Option.iter<TimeSpan> (fun x -> c.LingerMs <- Nullable (int x.TotalMilliseconds)) // default 0
        partitioner |> Option.iter (fun x -> c.Partitioner <- x)
        compression |> Option.iter (fun x -> c.CompressionType <- Nullable x)
        statisticsInterval |> Option.iter<TimeSpan> (fun x -> c.StatisticsIntervalMs <- Nullable (int x.TotalMilliseconds))
        custom |> Option.iter (fun xs -> for KeyValue (k,v) in xs do c.Set(k,v))
        customize |> Option.iter (fun f -> f c)
        KafkaProducerConfig(c, broker)

/// Creates and wraps a Confluent.Kafka Producer with the supplied configuration
type KafkaProducer private (inner : IProducer<string, string>, topic : string) =
    member __.Inner = inner
    member __.Topic = topic

    interface IDisposable with member __.Dispose() = inner.Dispose()

    /// Produces a single item, yielding a response upon completion/failure of the ack
    /// <remarks>
    ///     There's no assurance of ordering [without dropping `maxInFlight` down to `1` and annihilating throughput].
    ///     Thus its critical to ensure you don't submit another message for the same key until you've had a success / failure response from the call.<remarks/>
    member __.ProduceAsync(key, value) : Async<DeliveryResult<_,_>>= async {
        return! inner.ProduceAsync(topic, Message<_,_>(Key=key, Value=value)) |> Async.AwaitTaskCorrect }

    static member Create(log : ILogger, config : KafkaProducerConfig, topic : string): KafkaProducer =
        if String.IsNullOrEmpty topic then nullArg "topic"
        log.Information("Producing... {broker} / {topic} compression={compression} maxInFlight={maxInFlight} acks={acks}",
            config.Broker, topic, config.Compression, config.MaxInFlight, config.Acks)
        let p =
            ProducerBuilder<string, string>(config.Inner)
                .SetLogHandler(fun _p m -> log.Information("{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
                .SetErrorHandler(fun _p e -> log.Error("{reason} code={code} isBrokerError={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
                .Build()
        new KafkaProducer(p, topic)

type BatchedProducer private (log: ILogger, inner : IProducer<string, string>, topic : string) =
    member __.Inner = inner
    member __.Topic = topic

    interface IDisposable with member __.Dispose() = inner.Dispose()

    /// Produces a batch of supplied key/value messages. Results are returned in order of writing (which may vary from order of submission).
    /// <throws>
    ///    1. if there is an immediate local config issue
    ///    2. upon receipt of the first failed `DeliveryReport` (NB without waiting for any further reports, which can potentially leave some results in doubt should a 'batch' get split) </throws>
    /// <remarks>
    ///    Note that the delivery and/or write order may vary from the supplied order unless `maxInFlight` is 1 (which massively constrains throughput).
    ///    Thus it's important to note that supplying >1 item into the queue bearing the same key without maxInFlight=1 risks them being written out of order onto the topic.<remarks/>
    member __.ProduceBatch(keyValueBatch : (string * string)[]) = async {
        if Array.isEmpty keyValueBatch then return [||] else

        let! ct = Async.CancellationToken

        let tcs = new TaskCompletionSource<DeliveryReport<_,_>[]>()
        let numMessages = keyValueBatch.Length
        let results = Array.zeroCreate<DeliveryReport<_,_>> numMessages
        let numCompleted = ref 0

        use _ = ct.Register(fun _ -> tcs.TrySetCanceled() |> ignore)

        let handler (m : DeliveryReport<string,string>) =
            if m.Error.IsError then
                let errorMsg = exn (sprintf "Error on message topic=%s code=%O reason=%s" m.Topic m.Error.Code m.Error.Reason)
                tcs.TrySetException errorMsg |> ignore
            else
                let i = Interlocked.Increment numCompleted
                results.[i - 1] <- m
                if i = numMessages then tcs.TrySetResult results |> ignore 
        for key,value in keyValueBatch do
            inner.Produce(topic, Message<_,_>(Key=key, Value=value), deliveryHandler = handler)
        inner.Flush(ct)
        log.Debug("Produced {count}",!numCompleted)
        return! Async.AwaitTaskCorrect tcs.Task }

    /// Creates and wraps a Confluent.Kafka Producer that affords a batched production mode.
    /// The default settings represent a best effort at providing batched, ordered delivery semantics
    /// NB See caveats on the `ProduceBatch` API for further detail as to the semantics
    static member CreateWithConfigOverrides
        (   log : ILogger, config : KafkaProducerConfig, topic : string,
            /// Default: 1
            /// NB Having a <> 1 value for maxInFlight runs two risks due to the intrinsic lack of
            /// batching mechanisms within the Confluent.Kafka client:
            /// 1) items within the initial 'batch' can get written out of order in the face of timeouts and/or retries
            /// 2) items beyond the linger period may enter a separate batch, which can potentially get scheduled for transmission out of order
            ?maxInFlight,
            // This is used successfully in production with a 10ms linger value; having it in place is critical to items getting into the correct groupings. Default: 10ms
            ?linger: TimeSpan) : BatchedProducer =
        let lingerMs = match linger with Some x -> int x.TotalMilliseconds | None -> 100
        log.Information("Producing... Using batch Mode with linger={lingerMs}", lingerMs)
        config.Inner.LingerMs <- Nullable lingerMs
        config.Inner.MaxInFlight <- Nullable (defaultArg maxInFlight 1)
        let inner = KafkaProducer.Create(log, config, topic)
        new BatchedProducer(log, inner.Inner, topic)

type ConsumerBufferingConfig = { minInFlightBytes : int64; maxInFlightBytes : int64; maxBatchSize : int; maxBatchDelay : TimeSpan }

module Core =
    module Constants =
        let messageCounterSourceContext = "Jet.ConfluentKafka.FSharp.InFlightMessageCounter"

/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specfic settings
[<NoComparison>]
type KafkaConsumerConfig = private { inner: ConsumerConfig; topics: string list; buffering: ConsumerBufferingConfig } with
    /// Builds a Kafka Consumer Config suitable for KafkaConsumer.Start*
    static member Create
        (   /// Identify this consumer in logs etc
            clientId, broker : Uri, topics,
            /// Consumer group identifier.
            groupId,
            /// Specifies handling when Consumer Group does not yet have an offset recorded. Confluent.Kafka default: start from Latest. Default: start from Earliest.
            ?autoOffsetReset,
            /// Default 100kB.
            ?fetchMaxBytes,
            /// Minimum number of bytes to wait for (subject to timeout with default of 100ms). Default 1B.
            ?fetchMinBytes,
            /// Stats reporting interval for the consumer. Default: no reporting.
            ?statisticsInterval,
            /// Consumed offsets commit interval. Default 5s.
            ?offsetCommitInterval,
            /// Misc configuration parameter to be passed to the underlying CK consumer.
            ?custom,
            /// Postprocesses the ConsumerConfig after the rest of the rules have been applied
            ?customize,

            (* Client side batching *)

            /// Maximum number of messages to group per batch on consumer callbacks. Default 1000.
            ?maxBatchSize,
            /// Message batch linger time. Default 500ms.
            ?maxBatchDelay,
            /// Minimum total size of consumed messages in-memory for the consumer to attempt to fill. Default 16MiB.
            ?minInFlightBytes,
            /// Maximum total size of consumed messages in-memory before broker polling is throttled. Default 24MiB.
            ?maxInFlightBytes) =
        let c =
            ConsumerConfig(
                ClientId=clientId, BootstrapServers=Config.validateBrokerUri broker, GroupId=groupId,
                AutoOffsetReset = Nullable (defaultArg autoOffsetReset AutoOffsetReset.Earliest), // default: latest
                FetchMaxBytes = Nullable (defaultArg fetchMaxBytes 100_000), // default: 524_288_000
                MessageMaxBytes = Nullable (defaultArg fetchMaxBytes 100_000), // default 1_000_000
                EnableAutoCommit = Nullable true, // at AutoCommitIntervalMs interval, write value supplied by StoreOffset call
                EnableAutoOffsetStore = Nullable false, // explicit calls to StoreOffset are the only things that effect progression in offsets
                LogConnectionClose = Nullable false) // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
        fetchMinBytes |> Option.iter (fun x -> c.FetchMinBytes <- x) // Fetch waits for this amount of data for up to FetchWaitMaxMs (100)
        offsetCommitInterval |> Option.iter<TimeSpan> (fun x -> c.AutoCommitIntervalMs <- Nullable <| int x.TotalMilliseconds)
        statisticsInterval |> Option.iter<TimeSpan> (fun x -> c.StatisticsIntervalMs <- Nullable <| int x.TotalMilliseconds)
        custom |> Option.iter<seq<KeyValuePair<string,string>>> (fun xs -> for KeyValue (k,v) in xs do c.Set(k,v))
        customize |> Option.iter<ConsumerConfig -> unit> (fun f -> f c)
        {   inner = c 
            topics = match Seq.toList topics with [] -> invalidArg "topics" "must be non-empty collection" | ts -> ts
            buffering = {
                maxBatchSize = defaultArg maxBatchSize 1000
                maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromMilliseconds 500.)
                minInFlightBytes = defaultArg minInFlightBytes (16L * 1024L * 1024L)
                maxInFlightBytes = defaultArg maxInFlightBytes (24L * 1024L * 1024L) } }

// Stats format: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
type KafkaPartitionMetrics =
    {   partition: int
        [<JsonProperty("fetch_state")>]
        fetchState: string
        [<JsonProperty("next_offset")>]
        nextOffset: int64
        [<JsonProperty("stored_offset")>]
        storedOffset: int64
        [<JsonProperty("committed_offset")>]
        committedOffset: int64
        [<JsonProperty("lo_offset")>]
        loOffset: int64
        [<JsonProperty("hi_offset")>]
        hiOffset: int64
        [<JsonProperty("consumer_lag")>]
        consumerLag: int64 }        

type ConsumerBuilder =
    static member WithLogging(log : ILogger, config, ?onRevoke) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        log.Information("Consuming... {broker} {topics} {groupId} autoOffsetReset={autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlightBytes={maxInFlightB} maxBatchSize={maxBatchB} maxBatchDelay={maxBatchDelay}s",
            config.inner.BootstrapServers, config.topics, config.inner.GroupId, (let x = config.inner.AutoOffsetReset in x.Value), config.inner.FetchMaxBytes,
            config.buffering.maxInFlightBytes, config.buffering.maxBatchSize, (let t = config.buffering.maxBatchDelay in t.TotalSeconds))
        let consumer =
            ConsumerBuilder<_,_>(config.inner)
                .SetLogHandler(fun _c m -> log.Information("consumer_info|{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
                .SetErrorHandler(fun _c e -> log.Error("Consuming... Error reason={reason} code={code} broker={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
                .SetStatisticsHandler(fun _c json -> 
                    // Stats format: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
                    let stats = JToken.Parse json
                    for t in stats.Item("topics").Children() do
                        if t.HasValues && config.topics |> Seq.exists (fun ct -> ct = t.First.Item("topic").ToString()) then
                            let topic, partitions = let tm = t.First in tm.Item("topic").ToString(), tm.Item("partitions").Children()
                            let metrics = seq {
                                for tm in partitions do
                                    if tm.HasValues then
                                        let kpm = tm.First.ToObject<KafkaPartitionMetrics>()
                                        if kpm.partition <> -1 then
                                            yield kpm }                
                            log.Information("Consuming... Stats {topic:l} {@stats}", topic, metrics))
                .SetPartitionsAssignedHandler(fun _c xs ->
                    for topic,partitions in xs |> Seq.groupBy (fun p -> p.Topic) |> Seq.map (fun (t,ps) -> t, [| for p in ps -> let p = p.Partition in p.Value |]) do
                        log.Information("Consuming... Assigned {topic:l} {partitions}", topic, partitions))
                .SetPartitionsRevokedHandler(fun _c xs ->
                    for topic,partitions in xs |> Seq.groupBy (fun p -> p.Topic) |> Seq.map (fun (t,ps) -> t, [| for p in ps -> let p = p.Partition in p.Value |]) do
                        log.Information("Consuming... Revoked {topic:l} {partitions}", topic, partitions)
                    onRevoke |> Option.iter (fun f -> f xs))
                .SetOffsetsCommittedHandler(fun _c cos ->
                    for t,ps in cos.Offsets |> Seq.groupBy (fun p -> p.Topic) do
                        let o = [for p in ps -> let pp = p.Partition in pp.Value, let o = p.Offset in if o.IsSpecial then box (string o) else box o.Value(*, fmtError p.Error*)]
                        let e = cos.Error
                        if not e.IsError then log.Information("Consuming... Committed {topic} {@offsets}", t, o)
                        else log.Warning("Consuming... Committed {topic} {@offsets} reason={error} code={code} isBrokerError={isBrokerError}", t, o, e.Reason, e.Code, e.IsBrokerError))
                .Build()
        consumer.Subscribe config.topics
        consumer

module private ConsumerImpl =
    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    type BlockingCollection<'T> with
        member bc.FillBuffer(buffer : 'T[], maxDelay : TimeSpan) : int =
            let cts = new CancellationTokenSource()
            do cts.CancelAfter maxDelay

            let n = buffer.Length
            let mutable i = 0
            let mutable t = Unchecked.defaultof<'T>

            while i < n && not cts.IsCancellationRequested do
                if bc.TryTake(&t, 5 (* ms *)) then
                    buffer.[i] <- t ; i <- i + 1
                    while i < n && not cts.IsCancellationRequested && bc.TryTake(&t) do 
                        buffer.[i] <- t ; i <- i + 1
            i

    type PartitionedBlockingCollection<'Key, 'Message when 'Key : equality>(?perPartitionCapacity : int) =
        let collections = new ConcurrentDictionary<'Key, Lazy<BlockingCollection<'Message>>>()
        let onPartitionAdded = new Event<'Key * BlockingCollection<'Message>>()

        let createCollection() =
            match perPartitionCapacity with
            | None -> new BlockingCollection<'Message>()
            | Some c -> new BlockingCollection<'Message>(boundedCapacity = c)

        [<CLIEvent>]
        member __.OnPartitionAdded = onPartitionAdded.Publish

        member __.Add (key : 'Key, message : 'Message) =
            let factory key = lazy(
                let coll = createCollection()
                onPartitionAdded.Trigger(key, coll)
                coll)

            let buffer = collections.GetOrAdd(key, factory)
            buffer.Value.Add message

        member __.Revoke(key : 'Key) =
            match collections.TryRemove key with
            | true, coll -> Task.Delay(10000).ContinueWith(fun _ -> coll.Value.CompleteAdding()) |> ignore
            | _ -> ()

    type InFlightMessageCounter(log: ILogger, minInFlightBytes : int64, maxInFlightBytes : int64) =
        do  if minInFlightBytes < 1L then invalidArg "minInFlightBytes" "must be positive value"
            if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
            if minInFlightBytes > maxInFlightBytes then invalidArg "maxInFlightBytes" "must be greater than minInFlightBytes"

        let mutable inFlightBytes = 0L

        member __.Add(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore

        member __.AwaitThreshold() =
            if inFlightBytes > maxInFlightBytes then
                log.Information("Consumer reached in-flight message threshold, breaking off polling, bytes={max}", inFlightBytes)
                while inFlightBytes > minInFlightBytes do Thread.Sleep 5
                log.Verbose "Consumer resuming polling"

    let mkBatchedMessageConsumer (log: ILogger) (buf : ConsumerBufferingConfig) (ct : CancellationToken) (consumer : IConsumer<string, string>)
            (partitionedCollection: PartitionedBlockingCollection<TopicPartition, ConsumeResult<string, string>>)
            (handler : ConsumeResult<string,string>[] -> Async<unit>) = async {
        let tcs = new TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer
        
        let mcLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let counter = new InFlightMessageCounter(mcLog, buf.minInFlightBytes, buf.maxInFlightBytes)

        // starts a tail recursive loop that dequeues batches for a given partition buffer and schedules the user callback
        let consumePartition (collection : BlockingCollection<ConsumeResult<string, string>>) =
            let buffer = Array.zeroCreate buf.maxBatchSize
            let nextBatch () =
                let count = collection.FillBuffer(buffer, buf.maxBatchDelay)
                if count <> 0 then log.Debug("Consuming {count}", count)
                let batch = Array.init count (fun i -> buffer.[i])
                Array.Clear(buffer, 0, count)
                batch

            let rec loop () = async {
                if not collection.IsCompleted then
                    try match nextBatch() with
                        | [||] -> ()
                        | batch ->
                            // run the handler function
                            do! handler batch

                            // store completed offsets
                            let lastItem = batch |> Array.maxBy (fun m -> let o = m.Offset in o.Value)
                            consumer.StoreOffset(lastItem)

                            // decrement in-flight message counter
                            let batchSize = batch |> Array.sumBy approximateMessageBytes
                            counter.Add -batchSize
                    with e ->
                        tcs.TrySetException e |> ignore
                        cts.Cancel()
                    do! loop() }

            Async.Start(loop(), cts.Token)

        use _ = partitionedCollection.OnPartitionAdded.Subscribe (fun (_key,buffer) -> consumePartition buffer)

        // run the consumer
        let ct = cts.Token
        try while not ct.IsCancellationRequested do
                counter.AwaitThreshold()
                try let message = consumer.Consume(ct) // NB TimeSpan overload yields AVEs on 1.0.0-beta2
                    if message <> null then
                        counter.Add(approximateMessageBytes message)
                        partitionedCollection.Add(message.TopicPartition, message)
                with| :? ConsumeException as e -> log.Warning(e, "Consuming ... exception {name}", consumer.Name)
                    | :? System.OperationCanceledException -> log.Warning("Consuming... cancelled {name}", consumer.Name)
        finally
            consumer.Close()
        
        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

/// Creates and wraps a Confluent.Kafka IConsumer, wrapping it to afford a batched consumption mode with implicit offset progression at the end of each
/// (parallel across partitions, sequenced/monotinic within) batch of processing carried out by the `partitionHandler`
/// Conclusion of the processing (when a `partionHandler` throws and/or `Stop()` is called) can be awaited via `AwaitCompletion()`
type BatchedConsumer private (log : ILogger, inner : IConsumer<string, string>, task : Task<unit>, cts : CancellationTokenSource) =
    member __.Inner = inner

    interface IDisposable with member __.Dispose() = __.Stop()
    /// Request cancellation of processing
    member __.Stop() =  
        log.Information("Consuming ... Stopping {name}", inner.Name)
        cts.Cancel()
    /// Inspects current status of processing task
    member __.Status = task.Status
    /// Asynchronously awaits until consumer stops or is faulted
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

    /// Starts a Kafka consumer with the provided configuration. Batches are grouped by topic partition.
    /// Batches belonging to the same topic partition will be scheduled sequentially and monotonically; however batches from different partitions can run concurrently.
    /// Completion of the `partitionHandler` saves the attained offsets so the auto-commit can mark progress; yielding an exception terminates the processing
    static member Start(log : ILogger, config : KafkaConsumerConfig, partitionHandler : ConsumeResult<string,string>[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        log.Information("Consuming... {broker} {topics} {groupId} autoOffsetReset={autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlightBytes={maxInFlightB} maxBatchSize={maxBatchB} maxBatchDelay={maxBatchDelay}s",
            config.inner.BootstrapServers, config.topics, config.inner.GroupId, (let x = config.inner.AutoOffsetReset in x.Value), config.inner.FetchMaxBytes,
            config.buffering.maxInFlightBytes, config.buffering.maxBatchSize, (let t = config.buffering.maxBatchDelay in t.TotalSeconds))

        let partitionedCollection = new ConsumerImpl.PartitionedBlockingCollection<TopicPartition, ConsumeResult<string, string>>()
        let onRevoke (xs : seq<TopicPartitionOffset>) = 
            for x in xs do
                partitionedCollection.Revoke(x.TopicPartition)
        let consumer = ConsumerBuilder.WithLogging(log, config, onRevoke = onRevoke)
        let cts = new CancellationTokenSource()
        let task = ConsumerImpl.mkBatchedMessageConsumer log config.buffering cts.Token consumer partitionedCollection partitionHandler |> Async.StartAsTask
        new BatchedConsumer(log, consumer, task, cts)

    /// Starts a Kafka consumer instance that schedules handlers grouped by message key. Additionally accepts a global degreeOfParallelism parameter
    /// that controls the number of handlers running concurrently across partitions for the given consumer instance.
    static member StartByKey(log: ILogger, config : KafkaConsumerConfig, degreeOfParallelism : int, keyHandler : ConsumeResult<_,_> [] -> Async<unit>) =
        let semaphore = new SemaphoreSlim(degreeOfParallelism)
        let partitionHandler (messages : ConsumeResult<_,_>[]) = async {
            return!
                messages
                |> Seq.groupBy (fun m -> m.Key)
                |> Seq.map (fun (_,gp) -> async { 
                    let! ct = Async.CancellationToken
                    let! _ = semaphore.WaitAsync ct |> Async.AwaitTaskCorrect
                    try do! keyHandler (Seq.toArray gp)
                    finally semaphore.Release() |> ignore })
                |> Async.Parallel
                |> Async.Ignore
        }

        BatchedConsumer.Start(log, config, partitionHandler)

namespace Jet.ConfluentKafka.FSharp

open ProjectorTemplate.Consumer // AwaitTaskCorrect

open Confluent.Kafka
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module private Helpers =

    /// Gathers stats relating to how many items of a given partition have been observed
    type PartitionStats() =
        let partitions = Dictionary<int,int64>()
        member __.Record(partitionId, ?weight) = 
            let weight = defaultArg weight 1L
            match partitions.TryGetValue partitionId with
            | true, catCount -> partitions.[partitionId] <- catCount + weight
            | false, _ -> partitions.[partitionId] <- weight
        member __.Clear() = partitions.Clear()
    #if NET461
        member __.StatsDescending = partitions |> Seq.map (|KeyValue|) |> Seq.sortBy (fun (_,s) -> -s)
    #else
        member __.StatsDescending = partitions |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd
    #endif

    /// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
    let intervalCheck (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        fun () ->
            let due = timer.ElapsedMilliseconds > max
            if due then timer.Restart()
            due

    /// Maintains a Stopwatch used to drive a periodic loop, computing the remaining portion of the period per invocation
    /// - `Some remainder` if the interval has time remaining
    /// - `None` if the interval has expired (and triggers restarting the timer)
    let intervalTimer (period : TimeSpan) =
        let timer = Stopwatch.StartNew()
        fun () ->
            match period - timer.Elapsed with
            | remainder when remainder.Ticks > 0L -> Some remainder
            | _ -> timer.Restart(); None

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    /// Can't figure out a cleaner way to shim it :(
    let tryPeek (x : Queue<_>) = if x.Count = 0 then None else Some (x.Peek())

/// Deals with dispatch and result handling, triggering completion callbacks as batches reach completed state
module Scheduling =

    /// Single instance per system; coordinates the dispatching of work, subject to the maxDop concurrent processors constraint
    /// Semaphore is allocated on queueing, deallocated on completion of the processing
    type Dispatcher(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag's thread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>()) 
        let dop = new SemaphoreSlim(maxDop)
        /// Attempt to dispatch the supplied task - returns false if processing is presently running at full capacity
        member __.TryAdd task =
            if dop.Wait 0 then work.Add task; true
            else false
        /// Loop that continuously drains the work queue
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            for workItem in work.GetConsumingEnumerable ct do
                Async.Start(async {
                try do! workItem
                // Release the capacity on conclusion of the processing (exceptions should not pass to this level but the correctness here is critical)
                finally dop.Release() |> ignore }) }

    /// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type Batch<'M> = { partitionId : int; messages: 'M []; onCompletion: unit -> unit }

    /// Thread-safe/lock-free batch-level processing state
    /// - referenced [indirectly, see `mkDispatcher`] among all task invocations for a given batch
    /// - scheduler loop continuously inspects oldest active instance per partition in order to infer attainment of terminal (completed or faulted) state
    [<NoComparison; NoEquality>]
    type WipBatch<'M> =
        {   mutable elapsedMs : int64 // accumulated processing time for stats
            mutable remaining : int // number of outstanding completions; 0 => batch is eligible for completion
            mutable faults : ConcurrentStack<exn> // exceptions, order is not relevant and use is infrequent hence ConcurrentStack
            batch: Batch<'M> }
        member private __.RecordOk(duration : TimeSpan) =
            // need to record stats first as remaining = 0 is used as completion gate
            Interlocked.Add(&__.elapsedMs, int64 duration.TotalMilliseconds + 1L) |> ignore
            Interlocked.Decrement(&__.remaining) |> ignore
        member private __.RecordExn(_duration, exn) =
            __.faults.Push exn
        /// Prepares an initial set of shared state for a batch of tasks, together with the Async<unit> computations that will feed their results into it
        static member Create(batch : Batch<'M>, handle) : WipBatch<'M> * seq<Async<unit>> =
            let x = { elapsedMs = 0L; remaining = batch.messages.Length; faults = ConcurrentStack(); batch = batch }
            x, seq {
                for item in batch.messages -> async {
                    let sw = Stopwatch.StartNew()
                    try let! res = handle item
                        let elapsed = sw.Elapsed
                        match res with
                        | Choice1Of2 () -> x.RecordOk elapsed
                        | Choice2Of2 exn -> x.RecordExn(elapsed, exn)
                    // This exception guard _should_ technically not be necessary per the interface contract, but cannot risk an orphaned batch
                    with exn -> x.RecordExn(sw.Elapsed, exn) } }
    /// Infers whether a WipBatch is in a terminal state
    let (|Busy|Completed|Faulted|) = function
        | { remaining = 0; elapsedMs = ms } -> Completed (TimeSpan.FromMilliseconds <| float ms)
        | { faults = f } when not f.IsEmpty -> Faulted (f.ToArray())
        | _ -> Busy

    /// Continuously coordinates the propagation of incoming requests and mapping that to completed batches
    /// - replenishing the Dispatcher 
    /// - determining when WipBatches attain terminal state in order to triggering completion callbacks at the earliest possible opportunity
    /// - triggering abend of the processing should any dispatched tasks start to fault
    type PartitionedSchedulingEngine<'M>(log : ILogger, handle, tryDispatch : (Async<unit>) -> bool, statsInterval, ?logExternalStats) =
        // Submitters dictate batch commencement order by supply batches in a fair order; should never be empty if there is work in the system
        let incoming = ConcurrentQueue<Batch<'M>>()
        // Prepared work items ready to feed to Dispatcher (only created on demand in order to ensure we maximize overall progress and fairness)
        let waiting = Queue<Async<unit>>(1024)
        // Index of batches that have yet to attain terminal state (can be >1 per partition)
        let active = Dictionary<int(*partitionId*),Queue<WipBatch<'M>>>()
        (* accumulators for periodically emitted statistics info *)
        let mutable cycles, processingDuration = 0, TimeSpan.Zero
        let startedBatches, completedBatches, startedItems, completedItems = PartitionStats(), PartitionStats(), PartitionStats(), PartitionStats()
        let dumpStats () =
            let startedB, completedB = Array.ofSeq startedBatches.StatsDescending, Array.ofSeq completedBatches.StatsDescending
            let startedI, completedI = Array.ofSeq startedItems.StatsDescending, Array.ofSeq completedItems.StatsDescending
            let totalItemsCompleted = Array.sumBy snd completedI
            let latencyMs = match totalItemsCompleted with 0L -> null | cnt -> box (processingDuration.TotalMilliseconds / float cnt)
            log.Information("Scheduler {cycles} cycles Started {startedBatches}b {startedItems}i Completed {completedBatches}b {completedItems}i latency {completedLatency:f1}ms Ready {readyitems} Waiting {waitingBatches}b",
                cycles, Array.sumBy snd startedB, Array.sumBy snd startedI, Array.sumBy snd completedB, totalItemsCompleted, latencyMs, waiting.Count, incoming.Count)
            let active =
                seq { for KeyValue(pid,q) in active -> pid, q |> Seq.sumBy (fun x -> x.remaining) }
                |> Seq.filter (fun (_,snd) -> snd <> 0)
                |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Partitions Active items {@active} Started batches {@startedBatches} items {@startedItems} Completed batches {@completedBatches} items {@completedItems}",
                active, startedB, startedI, completedB, completedI)
            cycles <- 0; processingDuration <- TimeSpan.Zero; startedBatches.Clear(); completedBatches.Clear(); startedItems.Clear(); completedItems.Clear()
            logExternalStats |> Option.iter (fun f -> f log) // doing this in here allows stats intervals to be aligned with that of the scheduler engine
        let maybeLogStats : unit -> bool =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats (); true else false
        /// Inspects the oldest in-flight batch per partition to determine if it's reached a terminal state; if it has, remove and trigger completion callback
        let drainCompleted abend =
            let mutable more, worked = true, false
            while more do
                more <- false
                for queue in active.Values do
                    match tryPeek queue with
                    | None // empty
                    | Some Busy -> () // still working
                    | Some (Faulted exns) -> // outer layers will react to this by tearing us down
                        abend (AggregateException(exns))
                    | Some (Completed batchProcessingDuration) -> // call completion function asap
                        let partitionId, markCompleted, itemCount =
                            let { batch = { partitionId = p; onCompletion = f; messages = msgs } } = queue.Dequeue()
                            p, f, msgs.LongLength
                        completedBatches.Record partitionId
                        completedItems.Record(partitionId, itemCount)
                        processingDuration <- processingDuration.Add batchProcessingDuration
                        markCompleted ()
                        worked <- true
                        more <- true // vote for another iteration as the next one could already be complete too. Not looping inline/immediately to give others partitions equal credit
            worked
        /// Unpacks a new batch from the queue; each item goes through the `waiting` queue as the loop will continue to next iteration if dispatcher is full
        let tryPrepareNext () =
            match incoming.TryDequeue() with
            | false, _ -> false
            | true, ({ partitionId = pid; messages = msgs} as batch) ->
                startedBatches.Record(pid)
                startedItems.Record(pid, msgs.LongLength)
                let wipBatch, runners = WipBatch.Create(batch, handle)
                runners |> Seq.iter waiting.Enqueue
                match active.TryGetValue pid with
                | false, _ -> let q = Queue(1024) in active.[pid] <- q; q.Enqueue wipBatch
                | true, q -> q.Enqueue wipBatch
                true
        /// Tops up the current work in progress
        let reprovisionDispatcher () =
            let mutable more, worked = true, false
            while more do
                match tryPeek waiting with
                | None -> // Crack open a new batch if we don't have anything ready
                    more <- tryPrepareNext ()
                | Some pending -> // Dispatch until we reach capacity if we do have something
                    if tryDispatch pending then
                        worked <- true
                        waiting.Dequeue() |> ignore
                    else // Stop when it's full up
                        more <- false 
            worked

        /// Main pumping loop; `abend` is a callback triggered by a faulted task which the outer controler can use to shut down the processing
        member __.Pump abend = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let hadResults = drainCompleted abend
                let queuedWork = reprovisionDispatcher ()
                let loggedStats = maybeLogStats ()
                if not hadResults && not queuedWork && not loggedStats then
                    Thread.Sleep 1 } // not Async.Sleep, we like this context and/or cache state if nobody else needs it

        /// Feeds a batch of work into the queue; the caller is expected to ensure sumbissions are timely to avoid starvation, but throttled to ensure fair ordering
        member __.Submit(batches : Batch<'M>) =
            incoming.Enqueue batches

/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Submission =

    /// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type Batch<'M> = { partitionId : int; onCompletion: unit -> unit; messages: 'M [] }

    /// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
    [<NoComparison>]
    type PartitionQueue<'B> = { submissions: SemaphoreSlim; queue : Queue<'B> } with
        member __.Append(batch) = __.queue.Enqueue batch
        static member Create(maxSubmits) = { submissions = new SemaphoreSlim(maxSubmits); queue = Queue(maxSubmits * 2) } 

    /// Holds the stream of incoming batches, grouping by partition
    /// Manages the submission of batches into the Scheduler in a fair manner
    type SubmissionEngine<'M,'B>
        (   log : ILogger, pumpInterval : TimeSpan, maxSubmitsPerPartition, mapBatch: (unit -> unit) -> Batch<'M> -> 'B, submitBatch : 'B -> int, statsInterval,
            ?tryCompactQueue) =
        let incoming = new BlockingCollection<Batch<'M>[]>(ConcurrentQueue())
        let buffer = Dictionary<int,PartitionQueue<'B>>()
        let mutable cycles, ingested, compacted = 0, 0, 0
        let submittedBatches,submittedMessages = PartitionStats(), PartitionStats()
        let dumpStats () =
            let waiting = seq { for x in buffer do if x.Value.queue.Count <> 0 then yield x.Key, x.Value.queue.Count } |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Submitter {cycles} cycles {compactions} compactions Ingested {ingested} Waiting {@waiting} Batches {@batches} Messages {@messages}",
                cycles, ingested, compacted, waiting, submittedBatches.StatsDescending, submittedMessages.StatsDescending)
            ingested <- 0; compacted <- 0; cycles <- 0; submittedBatches.Clear(); submittedMessages.Clear()
        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats ()
        // Loop, submitting 0 or 1 item per partition per iteration to ensure
        // - each partition has a controlled maximum number of entrants in the scheduler queue
        // - a fair ordering of batch submissions
        let propagate () =
            let mutable more, worked = true, false
            while more do
                more <- false
                for KeyValue(pi,pq) in buffer do
                    if pq.queue.Count <> 0 then
                        if pq.submissions.Wait(0) then
                            worked <- true
                            more <- true
                            let count = submitBatch <| pq.queue.Dequeue()
                            submittedBatches.Record(pi)
                            submittedMessages.Record(pi, int64 count)
            worked
        /// Take one timeslice worth of ingestion and add to relevant partition queues
        /// When ingested, we allow one propagation submission per partition
        let ingest (partitionBatches : Batch<'M>[]) =
            for { partitionId = pid } as batch in partitionBatches do
                let pq =
                    match buffer.TryGetValue pid with
                    | false, _ -> let t = PartitionQueue<_>.Create(maxSubmitsPerPartition) in buffer.[pid] <- t; t
                    | true, pq -> pq
                let mapped = mapBatch (fun () -> pq.submissions.Release |> ignore) batch
                pq.Append(mapped)
            propagate()
        /// We use timeslices where we're we've fully provisioned the scheduler to index any waiting Batches
        let compact f =
            compacted <- compacted + 1
            let mutable worked = false
            for KeyValue(_,pq) in buffer do
                if f pq.queue then
                    worked <- true
            worked

        /// Processing loop, continuously splitting `Submit`ted items into per-partition queues and ensuring enough items are provided to the Scheduler
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable items = Unchecked.defaultof<_>
                let mutable propagated = false
                if incoming.TryTake(&items, pumpInterval) then
                    propagated <- ingest items
                    while incoming.TryTake(&items) do
                        if ingest items then propagated <- true
                match propagated, tryCompactQueue with
                | false, None -> Thread.Sleep 2
                | false, Some f when not (compact f) -> Thread.Sleep 2
                | _ -> ()

                maybeLogStats () }

        /// Supplies an incoming Batch for holding and forwarding to scheduler at the right time
        member __.Submit(items : Batch<'M>[]) =
            Interlocked.Increment(&ingested) |> ignore
            incoming.Add items

/// Manages efficiently and continuously reading from the Confluent.Kafka consumer, offloading the pushing of those batches onward to the Submitter
/// Responsible for ensuring we over-read, which would cause the rdkafka buffers to overload the system in terms of memory usage
module Ingestion =

    /// Accounts for the number/weight of messages currrently in the system so rdkafka reading doesn't get too far ahead
    type InFlightMessageCounter(log: ILogger, minInFlightBytes : int64, maxInFlightBytes : int64) =
        do  if minInFlightBytes < 1L then invalidArg "minInFlightBytes" "must be positive value"
            if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
            if minInFlightBytes > maxInFlightBytes then invalidArg "maxInFlightBytes" "must be greater than minInFlightBytes"

        let mutable inFlightBytes = 0L
        member __.InFlightMb = float inFlightBytes / 1024. / 1024.
        member __.Delta(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore
        member __.IsOverLimitNow() = Volatile.Read(&inFlightBytes) > maxInFlightBytes
        member __.AwaitThreshold() =
            log.Warning("Consumer reached in-flight message threshold, breaking off polling, bytes={max}", inFlightBytes)
            while __.IsOverLimitNow() do
                Thread.Sleep 2
            log.Information "Consumer resuming polling"

    /// Retains the messages we've accumulated for a given Partition
    [<NoComparison>]
    type PartitionSpan<'M> =
        {   mutable reservation : int64 // accumulate reserved in flight bytes so we can reverse the reservation when it completes
            mutable highWaterMark : ConsumeResult<string,string> // hang on to it so we can generate a checkpointing lambda
            messages : ResizeArray<'M> }
        member __.Append(sz, message, mapMessage) =
            __.highWaterMark <- message 
            __.reservation <- __.reservation + sz // size we need to unreserve upon completion
            __.messages.Add(mapMessage message)
        static member Create(sz,message,mapMessage) =
            let x = { reservation = 0L; highWaterMark = null; messages = ResizeArray(256) }
            x.Append(sz, message, mapMessage)
            x

    /// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accummulated messages as
    ///   checkpointable Batches
    /// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
    type IngestionEngine<'M>
        (   log : ILogger, counter : InFlightMessageCounter, consumer : IConsumer<_,_>, mapMessage : ConsumeResult<_,_> -> 'M, emit : Submission.Batch<'M>[] -> unit,
            emitInterval, statsInterval) =
        let acc = Dictionary()
        let remainingIngestionWindow = intervalTimer emitInterval
        let mutable intervalMsgs, intervalChars, totalMessages, totalChars = 0L, 0L, 0L, 0L
        let dumpStats () =
            totalMessages <- totalMessages + intervalMsgs; totalChars <- totalChars + intervalChars
            log.Information("Ingested {msgs:n0} messages, {chars:n0} chars In-flight ~{inflightMb:n1}MB Total {totalMessages:n0} messages {totalChars:n0} chars",
                intervalMsgs, intervalChars, counter.InFlightMb, totalMessages, totalChars)
            intervalMsgs <- 0L; intervalChars <- 0L
        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () -> if due () then dumpStats ()
        let ingest message =
            let sz = approximateMessageBytes message
            counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
            intervalMsgs <- intervalMsgs + 1L
            intervalChars <- intervalChars + int64 (message.Key.Length + message.Value.Length)
            let partitionId = let p = message.Partition in p.Value
            match acc.TryGetValue partitionId with
            | false, _ -> acc.[partitionId] <- PartitionSpan<'M>.Create(sz,message,mapMessage)
            | true, span -> span.Append(sz,message,mapMessage)
        let submit () =
            match acc.Count with
            | 0 -> ()
            | partitionsWithMessagesThisInterval ->
                let tmp = ResizeArray<Submission.Batch<'M>>(partitionsWithMessagesThisInterval)
                for KeyValue(partitionIndex,span) in acc do
                    let checkpoint () =
                        counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, above
                        try consumer.StoreOffset(span.highWaterMark)
                        with e -> log.Error(e, "Consuming... storing offsets failed")
                    tmp.Add { partitionId = partitionIndex; onCompletion = checkpoint; messages = span.messages.ToArray() }
                acc.Clear()
                emit <| tmp.ToArray()
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            use _ = consumer // Dispose it at the end (NB but one has to Close first or risk AccessViolations etc)
            try while not ct.IsCancellationRequested do
                    match counter.IsOverLimitNow(), remainingIngestionWindow () with
                    | true, _ ->
                        submit()
                        maybeLogStats()
                        counter.AwaitThreshold()
                    | false, None ->
                        submit()
                        maybeLogStats()
                    | false, Some intervalRemainder ->
                        try match consumer.Consume(intervalRemainder) with
                            | null -> ()
                            | message -> ingest message
                        with| :? System.OperationCanceledException -> log.Warning("Consuming... cancelled")
                            | :? ConsumeException as e -> log.Warning(e, "Consuming... exception")
            finally
                submit () // We don't want to leak our reservations against the counter and want to pass of messages we ingested
                dumpStats () // Unconditional logging when completing
                consumer.Close() (* Orderly Close() before Dispose() is critical *) }

/// Consumption pipeline that attempts to maximize concurrency of `handle` invocations (up to `dop` concurrently).
/// Consumes according to the `config` supplied to `Start`, until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitCompletion()`.
type ParallelConsumer private (inner : IConsumer<string, string>, task : Task<unit>, triggerStop) =

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Provides access to the Confluent.Kafka interface directly
    member __.Inner = inner
    /// Inspects current status of processing task
    member __.Status = task.Status

    /// Request cancellation of processing
    member __.Stop() = triggerStop ()

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start(log : ILogger, config : KafkaConsumerConfig, mapResult, submit, pumpSubmitter, pumpScheduler, pumpDispatcher, statsInterval) =

        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = new Ingestion.InFlightMessageCounter(limiterLog, config.buffering.minInFlightBytes, config.buffering.maxInFlightBytes)
        let consumer = ConsumerBuilder.WithLogging(log, config) // teardown is managed by ingester.Pump()
        let ingester = Ingestion.IngestionEngine<'M>(log, limiter, consumer, mapResult, submit, emitInterval = config.buffering.maxBatchDelay, statsInterval = statsInterval)
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = new TaskCompletionSource<unit>()
        let start name f =
            let wrap (name : string) computation = async {
                try do! computation
                    log.Information("Exiting {name}", name)
                with e -> log.Fatal(e, "Abend from {name}", name) }
            Async.Start(wrap name f, ct)
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let machine = async {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)
            start "dispatcher" <| pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" <| pumpScheduler abend
            start "submitter" <| pumpSubmitter
            start "ingester" <| ingester.Pump()

            // await for either handler-driven abend or external cancellation via Stop()
            do! Async.AwaitTaskCorrect tcs.Task
        }
        let task = Async.StartAsTask machine
        let triggerStop () =
            log.Information("Consuming ... Stopping {name}", consumer.Name)
            cts.Cancel();  
        new ParallelConsumer(consumer, task, triggerStop)

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'M>
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, mapResult : (ConsumeResult<string,string> -> 'M), handle : ('M -> Async<Choice<unit,exn>>),
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5

        let dispatcher = Scheduling.Dispatcher maxDop
        let scheduler = Scheduling.PartitionedSchedulingEngine<'M>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let mapBatch onCompletion (x : Submission.Batch<_>) : Scheduling.Batch<'M> =
            let onCompletion' () = x.onCompletion(); onCompletion()
            { partitionId = x.partitionId; messages = x.messages; onCompletion = onCompletion'; } 
        let submitBatch (x : Scheduling.Batch<_>) : int =
            scheduler.Submit x
            x.messages.Length
        let submitter = Submission.SubmissionEngine(log, pumpInterval, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval)
        ParallelConsumer.Start(log, config, mapResult, submitter.Submit, submitter.Pump(), scheduler.Pump, dispatcher.Pump(), statsInterval)

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, handle : KeyValuePair<string,string> -> Async<unit>,
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let mapConsumeResult (x : ConsumeResult<string,string>) = KeyValuePair(x.Key, x.Value)
        ParallelConsumer.Start<KeyValuePair<string,string>>(log, config, maxDop, mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)