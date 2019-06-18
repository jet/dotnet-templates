﻿namespace ProjectorTemplate.Consumer

open Newtonsoft.Json
open ProjectorTemplate.Consumer.Codec
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

/// Gathers stats relating to how many items of a given category have been observed
type CatStats() =
    let partitions = Dictionary<string,int64>()
    member __.Ingest(cat,?weight) = 
        let weight = defaultArg weight 1L
        match partitions.TryGetValue cat with
        | true, catCount -> partitions.[cat] <- catCount + weight
        | false, _ -> partitions.[cat] <- weight
    member __.Clear() = partitions.Clear()
    member __.StatsDescending = partitions |> Seq.map (|KeyValue|) |> Seq.sortBy (fun (_,s) -> -s)

/// When processing streamwise, he handler is passed deduplicates spans of events per stream, with a guarantee of max 1
/// in-flight request per stream
module Streams =

    type SkuId = string

    let settings = JsonSerializerSettings()

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module SavedForLater =
        type Item =             { skuId : SkuId; dateSaved : DateTimeOffset }

        type Added =            { skus : SkuId []; dateSaved : DateTimeOffset }
        type Removed =          { skus : SkuId [] }
        type Merged =           { items : Item [] }

        type Event =
            /// Inclusion of another set of state in this one
            | Merged of Merged
            /// Removal of a set of skus
            | Removed of Removed
            /// Addition of a collection of skus to the list
            | Added of Added
            /// Clearing of the list
            | Cleared
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(settings)
        let tryDecode = tryDecode codec
        let [<Literal>] CategoryId = "SavedForLater"

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module Favorites =
        type Favorited =        { date: DateTimeOffset; skuId: SkuId }
        type Unfavorited =      { skuId: SkuId }

        type Event =
            | Favorited         of Favorited
            | Unfavorited       of Unfavorited
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(settings)
        let tryDecode = tryDecode codec
        let [<Literal>] CategoryId = "Favorites"

    type Stat = Faves of int | Saves of int | OtherCategory of string * int | OtherMessage of string

    // Maintains a denormalized view cache per stream (in-memory, unbounded). TODO: keep in a persistent store
    type InMemoryHandler() =
        let log = Log.ForContext<InMemoryHandler>()

        // events are handled concurrently across streams. Only a single Handle call will be in progress at any time per stream
        let faves,saves = ConcurrentDictionary<string, HashSet<_>>(), ConcurrentDictionary<string,string list>()

        let (|FavoritesEvents|SavedForLaterEvents|OtherCategory|UnknownMessage|) (streamName, span : Propulsion.Streams.StreamSpan<byte[]>) =
            // The StreamProjector mechanism trims any events that have already been handled based on the in-memory state
            let map f = span.Events |> Array.choose (f log streamName)
            match category streamName with
            | Category (Favorites.CategoryId, id) ->
                let s = match faves.TryGetValue id with true, value -> value | false, _ -> new HashSet<string>()
                FavoritesEvents (id, s, map Favorites.tryDecode)
            | Category (SavedForLater.CategoryId, id) ->
                let s = match saves.TryGetValue id with true, value -> value | false, _ -> []
                SavedForLaterEvents (id, s, map SavedForLater.tryDecode)
            | Category (categoryName, _) -> OtherCategory (categoryName, Array.length span.events)
            | Unknown streamName -> UnknownMessage streamName

        // each event is guaranteed to only be supplied once by virtue of having been passed through the Streams Scheduler
        member __.Handle(streamName : string, span : Propulsion.Streams.StreamSpan<_>) = async {
            match streamName, span with
            | OtherCategory (cat,count) -> return OtherCategory (cat, count)
            | UnknownMessage messageKey -> return OtherMessage messageKey
            | FavoritesEvents (id, s, xs) -> 
                let folder (s : HashSet<_>) = function
                    | Favorites.Favorited e -> s.Add(e.skuId) |> ignore; s
                    | Favorites.Unfavorited e -> s.Remove(e.skuId) |> ignore; s
                faves.[id] <- Array.fold folder s xs
                return Faves xs.Length
            | SavedForLaterEvents (id, s, xs) ->
                let remove (skus : string seq) (s : _ list) =
                    let removing = (HashSet skus).Contains
                    s |> List.where (not << removing)
                let add skus (s : _ list) =
                    List.append (List.ofArray skus) s
                let folder s = function
                    | SavedForLater.Cleared -> []
                    | SavedForLater.Added e -> add e.skus s
                    | SavedForLater.Removed e -> remove e.skus s
                    | SavedForLater.Merged e -> s |> remove [| for x in e.items -> x.skuId |] |> add [| for x in e.items -> x.skuId |]
                saves.[id] <- (s,xs) ||> Array.fold folder
                return Saves xs.Length
        }

        // Dump stats relating to how much information is being held - note it's likely for requests to be in flighht during the call
        member __.DumpState(log : ILogger) =
            log.Information(" Favorited {total}/{users}", faves.Values |> Seq.sumBy (fun x -> x.Count), faves.Count)
            log.Information(" SavedForLater {total}/{users}", saves.Values |> Seq.sumBy (fun x -> x.Length), saves.Count)

    type Stats(log, ?statsInterval, ?stateInterval) =
        inherit Propulsion.Kafka.StreamsConsumerStats<Stat>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))

        let mutable faves, saves = 0, 0
        let otherCats, otherKeys = Propulsion.Streams.Internal.CatStats(), Propulsion.Streams.Internal.CatStats()

        override __.HandleOk res = res |> function
            | Faves count -> faves <- faves + count
            | Saves count -> saves <- saves + count
            | OtherCategory (cat,count) -> otherCats.Ingest(cat, int64 count)
            | OtherMessage messageKey -> otherKeys.Ingest messageKey

        // Dump stats relating to the nature of the message processing throughput
        override __.DumpStats () =
            if faves <> 0 || saves <> 0 then
                log.Information(" Processed Faves {faves} Saves {s}", faves, saves)
                faves <- 0; saves <- 0
            if otherCats.Any || otherKeys.Any then
                log.Information(" Ignored Categories {ignoredCats} Streams {ignoredStreams}",
                    Seq.truncate 5 otherCats.StatsDescending, Seq.truncate 5 otherKeys.StatsDescending)
                otherCats.Clear(); otherKeys.Clear()

    let private enumStreamEvents(KeyValue (streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
        if streamName.StartsWith("#serial") then Seq.empty else
        let span = JsonConvert.DeserializeObject<Propulsion.Codec.NewtonsoftJson.RenderedSpan>(spanJson)
        Propulsion.Codec.NewtonsoftJson.RenderedSpan.enumStreamEvents span

    let start (config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig, degreeOfParallelism : int) =
        let log, handler = Log.ForContext<InMemoryHandler>(), InMemoryHandler()
        let stats = Stats(log, TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 5.)
        Propulsion.Kafka.StreamsConsumer.Start(
            log, config, degreeOfParallelism,
            enumStreamEvents, handler.Handle, stats, category,
            logExternalState=handler.DumpState)

/// When using parallel or batch processing, items are not grouped by stream and the concurrency management is different
module Messages =
    
    // We'll use the same event parsing logic, though it works a little different
    open Streams

    type Message = Faves of Favorites.Event | Saves of SavedForLater.Event | OtherCat of name : string * count : int | Unclassified of messageKey : string 

    type MessageInterpreter() =
        let log = Log.ForContext<MessageInterpreter>()

        /// Handles various category / eventType / payload types as produced by Equinox.Tool
        member __.Interpret(streamName, events) = seq {
            let tryDecode  f = tryDecode f log streamName
            match streamName with
            | Category (Favorites.CategoryId,_) -> yield! events |> Seq.choose (tryDecode Favorites.codec >> Option.map Faves)
            | Category (SavedForLater.CategoryId,_) -> yield! events |> Seq.choose (tryDecode SavedForLater.codec >> Option.map Saves)
            | Category (other,_) -> yield OtherCat (other, Seq.length events)
            | _ -> yield Unclassified streamName }

        //member __.EnumStreamEvents(KeyValue (streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =

        //    let span = JsonConvert.DeserializeObject<Propulsion.Kafka.Codec.RenderedSpan>(spanJson)
        //    Propulsion.Kafka.Codec.RenderedSpan.enumStreamEvents span

        member __.TryDecode(streamName : string, spanJson) = seq {
            if streamName.StartsWith("#serial") then () else
            let span = JsonConvert.DeserializeObject<Propulsion.Codec.NewtonsoftJson.RenderedSpan>(spanJson)
            yield! __.Interpret(streamName, span.Events) }

    type Processor() =
        let mutable favorited, unfavorited, saved, removed, cleared = 0, 0, 0, 0, 0
        let cats, keys = CatStats(), ConcurrentDictionary()

        member __.DumpStats(log : ILogger) =
            log.Information("Favorited {f} Unfavorited {u} Saved {s} Removed {r} Cleared {c} Keys {keyCount} Categories {@catCount}",
                favorited, unfavorited, saved, removed, cleared, keys.Count, Seq.truncate 5 cats.StatsDescending)
            favorited <- 0; unfavorited <- 0; saved <- 0; removed <- 0; cleared <- 0; cats.Clear(); keys.Clear()

        // NB outcomes will arrive concurrently, care is required
        member __.Handle = function
            | Faves (Favorites.Favorited _) -> Interlocked.Increment &favorited |> ignore
            | Faves (Favorites.Unfavorited _) -> Interlocked.Increment &unfavorited |> ignore
            | Saves (SavedForLater.Added e) -> Interlocked.Add(&saved,e.skus.Length) |> ignore
            | Saves (SavedForLater.Removed e) -> Interlocked.Add(&cleared,e.skus.Length) |> ignore
            | Saves (SavedForLater.Merged e) -> Interlocked.Add(&saved,e.items.Length) |> ignore
            | Saves (SavedForLater.Cleared) -> Interlocked.Increment(&cleared) |> ignore
            | OtherCat (cat,count) -> lock cats <| fun () -> cats.Ingest(cat, int64 count)
            | Unclassified messageKey -> keys.TryAdd(messageKey, ()) |> ignore

    type BatchesSync =
        /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
        /// Processing runs as a single Async computation per batch, which can work well where parallism is not relevant
        static member Start(config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig) =
            let log = Log.ForContext<BatchesSync>()
            let interpreter = MessageInterpreter()
            let handleBatch (msgs : Confluent.Kafka.ConsumeResult<_,_>[]) = async {
                let processor = Processor()
                for m in msgs do
                    for x in interpreter.TryDecode(m.Key, m.Value) do
                        processor.Handle x
                processor.DumpStats log }
            Jet.ConfluentKafka.FSharp.BatchedConsumer.Start(log, config, handleBatch)
    
    type Parallel =
        /// Starts a consumer that consumes a topic in streamed mode
        /// StreamingConsumer manages the parallelism, spreading individual messages out to Async tasks
        /// Optimal where each Message naturally lends itself to independent processing with no ordering constraints
        static member Start(config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig, degreeOfParallelism : int) =
            let log = Log.ForContext<Parallel>()
            let interpreter, processor = MessageInterpreter(), Processor()
            let handleMessage (KeyValue (streamName,eventsSpan)) = async {
                for x in interpreter.TryDecode(streamName,eventsSpan) do
                    processor.Handle x }
            Propulsion.Kafka.ParallelConsumer.Start(
                log, config, degreeOfParallelism, handleMessage,
                statsInterval = TimeSpan.FromSeconds 30., logExternalStats = processor.DumpStats)

    type BatchesAsync =
        /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
        /// Processing fans out as parallel Async computations (limited to max `degreeOfParallelism` concurrent tasks
        /// The messages in the batch emanate from a single partition and are all in sequence
        /// notably useful where there's an ability to share some processing cost across a batch of work by doing the processing in phases
        static member Start(config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig, degreeOfParallelism : int) =
            let log = Log.ForContext<BatchesAsync>()
            let dop = new SemaphoreSlim(degreeOfParallelism)
            let interpreter = MessageInterpreter()
            let handleBatch (msgs : Confluent.Kafka.ConsumeResult<_,_>[]) = async {
                let processor = Processor()
                let! _ =
                    seq { for x in msgs do yield! interpreter.TryDecode(x.Key, x.Value) }
                    |> Seq.map (fun x -> async { processor.Handle x } |> dop.Throttle)
                    |> Async.Parallel
                processor.DumpStats log }
            Jet.ConfluentKafka.FSharp.BatchedConsumer.Start(log, config, handleBatch)