namespace ConsumerTemplate

open FsCodec
open Propulsion.Internal
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

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

        type Item =             { skuId: SkuId; dateSaved: DateTimeOffset }

        type Added =            { skus: SkuId []; dateSaved: DateTimeOffset }
        type Removed =          { skus: SkuId [] }
        type Merged =           { items: Item [] }

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
            
        module Reactions =

            let dec = Streams.Codec.gen<Event>
            let [<return: Struct>] (|StreamName|_|) = function
                | FsCodec.StreamName.CategoryAndId (Category, id) -> ValueSome id
                | _ -> ValueNone 
            let [<return: Struct>] (|Parse|_|) = function
                | struct (StreamName id, _) & Streams.Decode dec events -> ValueSome (id, events)
                | _ -> ValueNone

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module Favorites =

        let [<Literal>] Category = "Favorites"
        let [<return: Struct>] (|StreamName|_|) = function
            | FsCodec.StreamName.CategoryAndId (Category, id) -> ValueSome id
            | _ -> ValueNone 

        type Favorited =        { date: DateTimeOffset; skuId: SkuId }
        type Unfavorited =      { skuId: SkuId }

        type Event =
            | Favorited         of Favorited
            | Unfavorited       of Unfavorited
            interface TypeShape.UnionContract.IUnionContract
        
        module Reactions =

            let dec = Streams.Codec.gen<Event>
            let [<return: Struct>] (|Parse|_|) = function
                | struct (StreamName id, _) & Streams.Decode dec events -> ValueSome (id, events)
                | _ -> ValueNone

    type Stat = Faves of int | Saves of int | OtherCategory of string * int

    // Maintains a denormalized view cache per stream (in-memory, unbounded). TODO: keep in a persistent store
    type InMemoryHandler() =

        // events are handled concurrently across streams. Only a single Handle call will be in progress at any time per stream
        let faves, saves = ConcurrentDictionary<string, HashSet<SkuId>>(), ConcurrentDictionary<string, SkuId list>()

        // The StreamProjector mechanism trims any events that have already been handled based on the in-memory state
        let (|FavoritesEvents|SavedForLaterEvents|OtherCategory|) = function
            | Favorites.Reactions.Parse (id, events) ->
                let s = match faves.TryGetValue id with true, value -> value | false, _ -> HashSet<SkuId>()
                FavoritesEvents (id, s, events)
            | SavedForLater.Reactions.Parse (id, events) ->
                let s = match saves.TryGetValue id with true, value -> value | false, _ -> []
                SavedForLaterEvents (id, s, events)
            | StreamName.CategoryAndId (categoryName, _), events -> OtherCategory struct (categoryName, Array.length events)

        // each event is guaranteed to only be supplied once by virtue of having been passed through the Streams Scheduler
        member _.Handle(streamName: StreamName, events: Propulsion.Sinks.Event[]) = async {
            match struct (streamName, events) with
            | OtherCategory (cat, count) ->
                return Propulsion.Sinks.StreamResult.AllProcessed, OtherCategory (cat, count)
            | FavoritesEvents (id, s, xs) ->
                let folder (s: HashSet<_>) = function
                    | Favorites.Favorited e -> s.Add(e.skuId) |> ignore; s
                    | Favorites.Unfavorited e -> s.Remove(e.skuId) |> ignore; s
                faves[id] <- Array.fold folder s xs
                return Propulsion.Sinks.StreamResult.AllProcessed, Faves xs.Length
            | SavedForLaterEvents (id, s, xs) ->
                let remove (skus: SkuId seq) (s: _ list) =
                    let removing = (HashSet skus).Contains
                    s |> List.where (not << removing)
                let add skus (s: _ list) =
                    List.append (List.ofArray skus) s
                let folder s = function
                    | SavedForLater.Cleared -> []
                    | SavedForLater.Added e -> add e.skus s
                    | SavedForLater.Removed e -> remove e.skus s
                    | SavedForLater.Merged e -> s |> remove [| for x in e.items -> x.skuId |] |> add [| for x in e.items -> x.skuId |]
                saves[id] <- (s, xs) ||> Array.fold folder
                return Propulsion.Sinks.StreamResult.AllProcessed, Saves xs.Length
        }

        // Dump stats relating to how much information is being held - note it's likely for requests to be in flighht during the call
        member _.DumpState(log: ILogger) =
            log.Information(" Favorited {total}/{users}", faves.Values |> Seq.sumBy (fun x -> x.Count), faves.Count)
            log.Information(" SavedForLater {total}/{users}", saves.Values |> Seq.sumBy (fun x -> x.Length), saves.Count)

    type Stats(log, statsInterval, stateInterval) =
        inherit Propulsion.Streams.Stats<Stat>(log, statsInterval, stateInterval)

        let mutable faves, saves = 0, 0
        let otherCats = Stats.CatStats()

        override _.HandleOk res = res |> function
            | Faves count -> faves <- faves + count
            | Saves count -> saves <- saves + count
            | OtherCategory (cat, count) -> otherCats.Ingest(cat, int64 count)
        override _.HandleExn(log, exn) =
            log.Information(exn, "Unhandled")

        // Dump stats relating to the nature of the message processing throughput
        override _.DumpStats() =
            base.DumpStats()
            if faves <> 0 || saves <> 0 then
                log.Information(" Processed Faves {faves} Saves {s}", faves, saves)
                faves <- 0; saves <- 0
            if otherCats.Any then
                log.Information(" Ignored Categories {ignoredCats}", Seq.truncate 5 otherCats.StatsDescending)
                otherCats.Clear()

    let private parseStreamEvents(res: Confluent.Kafka.ConsumeResult<_, _>): seq<Propulsion.Sinks.StreamEvent> =
        Propulsion.Codec.NewtonsoftJson.RenderedSpan.parse res.Message.Value

    let start (config: FsKafka.KafkaConsumerConfig, degreeOfParallelism: int) =
        let log, handler = Log.ForContext<InMemoryHandler>(), InMemoryHandler()
        let stats = Stats(log, TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 5.)
        Propulsion.Kafka.Factory.StartConcurrent(
            log, config, parseStreamEvents,
            degreeOfParallelism, (fun s ss -> handler.Handle(s, ss)), stats, logExternalState = handler.DumpState)

/// When using parallel or batch processing, items are not grouped by stream but there are no constraints on the concurrency
module MultiMessages =

    // We'll use the same event parsing logic, though it works a little differently
    open MultiStreams

    type Message = Fave of Favorites.Event | Save of SavedForLater.Event | OtherCat of name: string * count: int | Unclassified of messageKey: string

    type Processor() =
        let mutable favorited, unfavorited, saved, removed, cleared = 0, 0, 0, 0, 0
        let cats, keys = Stats.CatStats(), ConcurrentDictionary()

        // `BatchedConsumer` holds a `Processor` instance per in-flight batch (there will likely be a batch in flight per partition assigned to this consumer)
        //   and waits for the work to complete before calling this
        // `ParallelScheduler` ensures that only one call to `logExternalStats` will take place at a time, but it's highly likely that the execution will
        //   overlap with a call to `Handle` (which makes for a slight race condition between the capturing of the values in the log statement and the resetting)
        member _.DumpStats(log: ILogger) =
            log.Information("Favorited {f} Unfavorited {u} Saved {s} Removed {r} Cleared {c} Keys {keyCount} Categories {@catCount}",
                favorited, unfavorited, saved, removed, cleared, keys.Count, Seq.truncate 5 cats.StatsDescending)
            favorited <- 0; unfavorited <- 0; saved <- 0; removed <- 0; cleared <- 0; cats.Clear(); keys.Clear()

        /// Handles various category / eventType / payload types as produced by Equinox.Tool
        member private _.Interpret(streamName: StreamName, spanJson): seq<Message> = seq {
            let raw =
                Propulsion.Codec.NewtonsoftJson.RenderedSpan.Parse spanJson
                |> Propulsion.Codec.NewtonsoftJson.RenderedSpan.enum
                |> Seq.map ValueTuple.snd
            match struct (streamName, Array.ofSeq raw) with
            | Favorites.Reactions.Parse (_, events) -> yield! events |> Seq.map Fave
            | SavedForLater.Reactions.Parse (_, events) -> yield! events |> Seq.map Save
            | StreamName.CategoryAndId (otherCategoryName, _), events -> yield OtherCat (otherCategoryName, events.Length) }

        // NB can be called in parallel, so must be thread-safe
        member x.Handle(streamName: StreamName, spanJson: string) =
            for x in x.Interpret(streamName, spanJson) do
                match x with
                | Fave (Favorites.Favorited _) -> Interlocked.Increment &favorited |> ignore
                | Fave (Favorites.Unfavorited _) -> Interlocked.Increment &unfavorited |> ignore
                | Save (SavedForLater.Added e) -> Interlocked.Add(&saved, e.skus.Length) |> ignore
                | Save (SavedForLater.Removed e) -> Interlocked.Add(&cleared, e.skus.Length) |> ignore
                | Save (SavedForLater.Merged e) -> Interlocked.Add(&saved, e.items.Length) |> ignore
                | Save SavedForLater.Cleared -> Interlocked.Increment(&cleared) |> ignore
                | OtherCat (cat, count) -> lock cats <| fun () -> cats.Ingest(cat, int64 count)
                | Unclassified messageKey -> keys.TryAdd(messageKey, ()) |> ignore

    type Parallel =
        /// Starts a consumer that consumes a topic in streamed mode
        /// StreamingConsumer manages the parallelism, spreading individual messages out to Async tasks
        /// Optimal where each Message naturally lends itself to independent processing with no ordering constraints
        static member Start(config: FsKafka.KafkaConsumerConfig, degreeOfParallelism: int) =
            let log, processor = Log.ForContext<Parallel>(), Processor()
            let handleMessage (KeyValue (streamName, eventsSpan)) _ct = task { processor.Handle(StreamName.parse streamName, eventsSpan) }
            Propulsion.Kafka.ParallelConsumer.Start(
                log, config, degreeOfParallelism, handleMessage,
                statsInterval=TimeSpan.FromSeconds 30., logExternalStats=processor.DumpStats)

    type BatchesSync =
        /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
        /// Processing runs as a single Async computation per batch, which can work well where parallelism is not relevant
        static member Start(config: FsKafka.KafkaConsumerConfig) =
            let log = Log.ForContext<BatchesSync>()
            let handleBatch (msgs: Confluent.Kafka.ConsumeResult<_, _>[]) = async {
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
        static member Start(config: FsKafka.KafkaConsumerConfig, degreeOfParallelism: int) =
            let log = Log.ForContext<BatchesAsync>()
            let dop = new SemaphoreSlim(degreeOfParallelism)
            let handleBatch (msgs: Confluent.Kafka.ConsumeResult<_, _>[]) = async {
                let processor = Processor()
                let! _ = Async.Parallel(seq { for m in msgs -> async { processor.Handle(StreamName.parse m.Message.Key, m.Message.Value) } |> dop.Throttle })
                processor.DumpStats log }
            FsKafka.BatchedConsumer.Start(log, config, handleBatch)
