namespace ConsumerTemplate

open FsCodec
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

module EventCodec =

    /// Uses the supplied codec to decode the supplied event record `x` (iff at LogEventLevel.Debug, detail fails to `log` citing the `stream` and content)
    let tryDecode (codec : FsCodec.IEventCodec<_, _, _>) (log : ILogger) streamName (x : FsCodec.ITimelineEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, streamName)
            None
        | x -> x

/// This more advanced sample shows processing >1 category of events, and maintaining derived state based on it
// When processing streamwise, the handler is passed deduplicated spans of events per stream, with a guarantee of max 1
// in-flight request per stream, which allows one to avoid having to consider any explicit concurrency management
module MultiStreams =

    open FSharp.UMX // See https://github.com/fsprojects/FSharp.UMX
    [<Measure>] type skuId 
    type SkuId = string<skuId> // Only significant at compile time - serializers etc. just see a string

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module SavedForLater =

        let [<Literal>] Category = "SavedForLater"

        type Item =             { skuId : SkuId; dateSaved : DateTimeOffset }

        type Added =            { skus : SkuId []; dateSaved : DateTimeOffset }
        type Removed =          { skus : SkuId [] }
        type Merged =           { items : Item [] }

        type Event =
            /// Inclusion of another set of state in this one
            | Merged of Merged
            /// Removal of a set of skus
            | Removed of Removed
            /// Addition of a set of skus to the [head of the] list
            | Added of Added
            /// Clearing of the list
            | Cleared
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
        let tryDecode = EventCodec.tryDecode codec

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module Favorites =

        let [<Literal>] Category = "Favorites"

        type Favorited =        { date: DateTimeOffset; skuId: SkuId }
        type Unfavorited =      { skuId: SkuId }

        type Event =
            | Favorited         of Favorited
            | Unfavorited       of Unfavorited
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
        let tryDecode = EventCodec.tryDecode codec

    type Stat = Faves of int | Saves of int | OtherCategory of string * int

    // Maintains a denormalized view cache per stream (in-memory, unbounded). TODO: keep in a persistent store
    type InMemoryHandler() =
        let log = Log.ForContext<InMemoryHandler>()

        // events are handled concurrently across streams. Only a single Handle call will be in progress at any time per stream
        let faves, saves = ConcurrentDictionary<string, HashSet<SkuId>>(), ConcurrentDictionary<string, SkuId list>()

        // The StreamProjector mechanism trims any events that have already been handled based on the in-memory state
        let (|FavoritesEvents|SavedForLaterEvents|OtherCategory|) (streamName, span : Propulsion.Streams.StreamSpan<byte[]>) =
            let decode tryDecode = span.events |> Seq.choose (tryDecode log streamName) |> Array.ofSeq
            match streamName with
            | FsCodec.StreamName.CategoryAndId (Favorites.Category, id) ->
                let s = match faves.TryGetValue id with true, value -> value | false, _ -> new HashSet<SkuId>()
                FavoritesEvents (id, s, decode Favorites.tryDecode)
            | FsCodec.StreamName.CategoryAndId (SavedForLater.Category, id) ->
                let s = match saves.TryGetValue id with true, value -> value | false, _ -> []
                SavedForLaterEvents (id, s, decode SavedForLater.tryDecode)
            | FsCodec.StreamName.CategoryAndId (categoryName, _) -> OtherCategory (categoryName, Seq.length span.events)

        // each event is guaranteed to only be supplied once by virtue of having been passed through the Streams Scheduler
        member __.Handle(streamName : StreamName, span : Propulsion.Streams.StreamSpan<_>) = async {
            match streamName, span with
            | OtherCategory (cat, count) -> return OtherCategory (cat, count)
            | FavoritesEvents (id, s, xs) ->
                let folder (s : HashSet<_>) = function
                    | Favorites.Favorited e -> s.Add(e.skuId) |> ignore; s
                    | Favorites.Unfavorited e -> s.Remove(e.skuId) |> ignore; s
                faves.[id] <- Array.fold folder s xs
                return Faves xs.Length
            | SavedForLaterEvents (id, s, xs) ->
                let remove (skus : SkuId seq) (s : _ list) =
                    let removing = (HashSet skus).Contains
                    s |> List.where (not << removing)
                let add skus (s : _ list) =
                    List.append (List.ofArray skus) s
                let folder s = function
                    | SavedForLater.Cleared -> []
                    | SavedForLater.Added e -> add e.skus s
                    | SavedForLater.Removed e -> remove e.skus s
                    | SavedForLater.Merged e -> s |> remove [| for x in e.items -> x.skuId |] |> add [| for x in e.items -> x.skuId |]
                saves.[id] <- (s, xs) ||> Array.fold folder
                return Saves xs.Length
        }

        // Dump stats relating to how much information is being held - note it's likely for requests to be in flighht during the call
        member __.DumpState(log : ILogger) =
            log.Information(" Favorited {total}/{users}", faves.Values |> Seq.sumBy (fun x -> x.Count), faves.Count)
            log.Information(" SavedForLater {total}/{users}", saves.Values |> Seq.sumBy (fun x -> x.Length), saves.Count)

    type Stats(log, statsInterval, stateInterval) =
        inherit Propulsion.Kafka.StreamsConsumerStats<Stat>(log, statsInterval, stateInterval)

        let mutable faves, saves = 0, 0
        let otherCats = Propulsion.Streams.Internal.CatStats()

        override __.HandleOk res = res |> function
            | Faves count -> faves <- faves + count
            | Saves count -> saves <- saves + count
            | OtherCategory (cat, count) -> otherCats.Ingest(cat, int64 count)

        // Dump stats relating to the nature of the message processing throughput
        override __.DumpStats () =
            if faves <> 0 || saves <> 0 then
                log.Information(" Processed Faves {faves} Saves {s}", faves, saves)
                faves <- 0; saves <- 0
            if otherCats.Any then
                log.Information(" Ignored Categories {ignoredCats}", Seq.truncate 5 otherCats.StatsDescending)
                otherCats.Clear()

    let private parseStreamEvents(KeyValue (_streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
        Propulsion.Codec.NewtonsoftJson.RenderedSpan.parse spanJson

    let start (config : FsKafka.KafkaConsumerConfig, degreeOfParallelism : int) =
        let log, handler = Log.ForContext<InMemoryHandler>(), InMemoryHandler()
        let stats = Stats(log, TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 5.)
        Propulsion.Kafka.StreamsConsumer.Start(
            log, config, parseStreamEvents, handler.Handle, degreeOfParallelism,
            stats, logExternalState=handler.DumpState)

/// When using parallel or batch processing, items are not grouped by stream but there are no constraints on the concurrency
module MultiMessages =
    
    // We'll use the same event parsing logic, though it works a little differently
    open MultiStreams
    open Propulsion.Codec.NewtonsoftJson

    type Message = Fave of Favorites.Event | Save of SavedForLater.Event | OtherCat of name : string * count : int | Unclassified of messageKey : string

    type Processor() =
        let log = Log.ForContext<Processor>()
        let mutable favorited, unfavorited, saved, removed, cleared = 0, 0, 0, 0, 0
        let cats, keys = Propulsion.Streams.Internal.CatStats(), ConcurrentDictionary()

        // `BatchedConsumer` holds a `Processor` instance per in-flight batch (there will likely be a batch in flight per partition assigned to this consumer)
        //   and waits for the work to complete before calling this
        // `ParallelScheduler` ensures that only one call to `logExternalStats` will take place at a time, but it's highly likely that the execution will
        //   overlap with a call to `Handle` (which makes for a slight race condition between the capturing of the values in the log statement and the resetting)
        member __.DumpStats(log : ILogger) =
            log.Information("Favorited {f} Unfavorited {u} Saved {s} Removed {r} Cleared {c} Keys {keyCount} Categories {@catCount}",
                favorited, unfavorited, saved, removed, cleared, keys.Count, Seq.truncate 5 cats.StatsDescending)
            favorited <- 0; unfavorited <- 0; saved <- 0; removed <- 0; cleared <- 0; cats.Clear(); keys.Clear()

        /// Handles various category / eventType / payload types as produced by Equinox.Tool
        member private __.Interpret(streamName : StreamName, spanJson) : seq<Message> = seq {
            let span = Propulsion.Codec.NewtonsoftJson.RenderedSpan.Parse spanJson
            let decode tryDecode wrap = RenderedSpan.enum span |> Seq.choose (fun x -> x.event |> tryDecode log streamName |> Option.map wrap)
            match streamName with
            | StreamName.CategoryAndId (Favorites.Category, _) -> yield! decode Favorites.tryDecode Fave
            | StreamName.CategoryAndId (SavedForLater.Category, _) -> yield! decode SavedForLater.tryDecode Save
            | StreamName.CategoryAndId (otherCategoryName, _) -> yield OtherCat (otherCategoryName, Seq.length span.e) }

        // NB can be called in parallel, so must be thread-safe
        member __.Handle(streamName : StreamName, spanJson : string) =
            for x in __.Interpret(streamName, spanJson) do
                match x with
                | Fave (Favorites.Favorited _) -> Interlocked.Increment &favorited |> ignore
                | Fave (Favorites.Unfavorited _) -> Interlocked.Increment &unfavorited |> ignore
                | Save (SavedForLater.Added e) -> Interlocked.Add(&saved, e.skus.Length) |> ignore
                | Save (SavedForLater.Removed e) -> Interlocked.Add(&cleared, e.skus.Length) |> ignore
                | Save (SavedForLater.Merged e) -> Interlocked.Add(&saved, e.items.Length) |> ignore
                | Save (SavedForLater.Cleared) -> Interlocked.Increment(&cleared) |> ignore
                | OtherCat (cat, count) -> lock cats <| fun () -> cats.Ingest(cat, int64 count)
                | Unclassified messageKey -> keys.TryAdd(messageKey, ()) |> ignore

    type Parallel =
        /// Starts a consumer that consumes a topic in streamed mode
        /// StreamingConsumer manages the parallelism, spreading individual messages out to Async tasks
        /// Optimal where each Message naturally lends itself to independent processing with no ordering constraints
        static member Start(config : FsKafka.KafkaConsumerConfig, degreeOfParallelism : int) =
            let log, processor = Log.ForContext<Parallel>(), Processor()
            let handleMessage (KeyValue (streamName, eventsSpan)) = async { processor.Handle(StreamName.parse streamName, eventsSpan) }
            Propulsion.Kafka.ParallelConsumer.Start(
                log, config, degreeOfParallelism, handleMessage,
                statsInterval = TimeSpan.FromSeconds 30., logExternalStats = processor.DumpStats)

    type BatchesSync =
        /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
        /// Processing runs as a single Async computation per batch, which can work well where parallelism is not relevant
        static member Start(config : FsKafka.KafkaConsumerConfig) =
            let log = Log.ForContext<BatchesSync>()
            let handleBatch (msgs : Confluent.Kafka.ConsumeResult<_, _>[]) = async {
                let processor = Processor()
                for m in msgs do
                    processor.Handle(StreamName.parse m.Message.Key, m.Message.Value)
                processor.DumpStats log }
            FsKafka.BatchedConsumer.Start(log, config, handleBatch)
    
    type BatchesAsync =
        /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
        /// Processing fans out as parallel Async computations (limited to max `degreeOfParallelism` concurrent tasks
        /// The messages in the batch emanate from a single partition and are all in sequence
        /// notably useful where there's an ability to share some processing cost across a batch of work by doing the processing in phases
        static member Start(config : FsKafka.KafkaConsumerConfig, degreeOfParallelism : int) =
            let log = Log.ForContext<BatchesAsync>()
            let dop = new SemaphoreSlim(degreeOfParallelism)
            let handleBatch (msgs : Confluent.Kafka.ConsumeResult<_, _>[]) = async {
                let processor = Processor()
                let! _ = Async.Parallel(seq { for m in msgs -> async { processor.Handle(StreamName.parse m.Message.Key, m.Message.Value) } |> dop.Throttle })
                processor.DumpStats log }
            FsKafka.BatchedConsumer.Start(log, config, handleBatch)