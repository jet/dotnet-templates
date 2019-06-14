namespace ProjectorTemplate.Consumer

open Newtonsoft.Json
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

[<AutoOpen>]
module EventParser =

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
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(settings)

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module Favorites =
        type Favorited =        { date: DateTimeOffset; skuId: SkuId }
        type Unfavorited =      { skuId: SkuId }

        type Event =
            | Favorited         of Favorited
            | Unfavorited       of Unfavorited
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(settings)
    
    let tryDecode (log : ILogger) (stream : string) (codec : Equinox.Codec.IUnionEncoder<_,_>) (x : Equinox.Codec.IEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream);
            None
        | Some e -> Some e

type Message = Faves of Favorites.Event | Saves of SavedForLater.Event | Category of name : string * count : int | Unclassified of messageKey : string 

type EquinoxEvent =
    static member Parse (x: Propulsion.Codec.NewtonsoftJson.RenderedEvent) =
        { new Equinox.Codec.IEvent<_> with
            member __.EventType = x.c
            member __.Data = x.d
            member __.Meta = x.m
            member __.Timestamp = x.t }
    static member Parse (x: Propulsion.Streams.IEvent<_>) =
        { new Equinox.Codec.IEvent<_> with
            member __.EventType = x.EventType
            member __.Data = x.Data
            member __.Meta = x.Meta
            member __.Timestamp = x.Timestamp }

type EquinoxSpan =
    static member EnumCodecEvents (x: Propulsion.Codec.NewtonsoftJson.RenderedSpan) : seq<Equinox.Codec.IEvent<_>> =
       x.e |> Seq.map EquinoxEvent.Parse
    static member EnumCodecEvents (x: Propulsion.Streams.StreamSpan<_>) : seq<Equinox.Codec.IEvent<_>> =
       x.events |> Seq.map EquinoxEvent.Parse

// Example of filtering our relevant Events from the Kafka stream
// NB if the percentage of relevant events is low, one may wish to adjust the projector to project only a subset
type MessageInterpreter() =
    let log = Log.ForContext<MessageInterpreter>()

    /// Handles various category / eventType / payload types as produced by Equinox.Tool
    member __.Interpret(streamName, events) = seq {
        let tryExtractCategory (stream : string) = stream.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries)
        match tryExtractCategory streamName with
        | [| "Favorites"; _ |] -> yield! events |> Seq.choose (tryDecode log streamName Favorites.codec >> Option.map Faves)
        | [| "SavedForLater"; _ |] -> yield! events |> Seq.choose (tryDecode log streamName SavedForLater.codec >> Option.map Saves)
        | [| category; _ |] -> yield Category (category, Seq.length events)
        | _ -> yield Unclassified streamName }

    member __.EnumStreamEvents(KeyValue (streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
        if streamName.StartsWith("#serial") then Seq.empty else

        let span = JsonConvert.DeserializeObject<Propulsion.Codec.NewtonsoftJson.RenderedSpan>(spanJson)
        Propulsion.Codec.NewtonsoftJson.RenderedSpan.enumStreamEvents span

    /// Handles various category / eventType / payload types as produced by Equinox.Tool
    member __.TryDecode(streamName, spanJson) = seq {
        let span = JsonConvert.DeserializeObject<Propulsion.Codec.NewtonsoftJson.RenderedSpan>(spanJson)
        yield! __.Interpret(streamName, EquinoxSpan.EnumCodecEvents span) }

type Processor() =
    let mutable favorited, unfavorited, saved, cleared = 0, 0, 0, 0 
    let cats, keys = CatStats(), ConcurrentDictionary()

    member __.DumpStats(log : ILogger) =
        log.Information("Favorited {f} Unfavorited {u} Saved {s} Cleared {c} Keys {keyCount} Categories {@catCount}",
            favorited, unfavorited, saved, cleared, keys.Count, Seq.truncate 5 cats.StatsDescending)
        favorited <- 0; unfavorited <- 0; saved <- 0; cleared <- 0; cats.Clear(); keys.Clear()
    member __.Handle = function
        | Faves (Favorites.Favorited _) -> Interlocked.Increment &favorited |> ignore
        | Faves (Favorites.Unfavorited _) -> Interlocked.Increment &unfavorited |> ignore
        | Saves (SavedForLater.Added e) -> Interlocked.Add(&saved,e.skus.Length) |> ignore
        | Saves (SavedForLater.Removed e) -> Interlocked.Add(&cleared,e.skus.Length) |> ignore
        | Saves (SavedForLater.Merged e) -> Interlocked.Add(&saved,e.items.Length) |> ignore
        | Category (cat,count) -> lock cats <| fun () -> cats.Ingest(cat, int64 count)
        | Unclassified messageKey -> keys.TryAdd(messageKey, ()) |> ignore

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Propulsion.Kafka

type BatchesSync =
    /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
    /// Processing runs as a single Async computation per batch, which can work well where parallism is not relevant
    static member Start(config : KafkaConsumerConfig) =
        let log = Log.ForContext<BatchesSync>()
        let interpreter = MessageInterpreter()
        let handleBatch (msgs : ConsumeResult<_,_>[]) = async {
            let processor = Processor()
            for m in msgs do
                for x in interpreter.TryDecode(m.Key, m.Value) do
                    processor.Handle x
            processor.DumpStats log }
        BatchedConsumer.Start(log, config, handleBatch)
        
type Messages =
    /// Starts a consumer that consumes a topic in streamed mode
    /// StreamingConsumer manages the parallelism, spreading individual messages out to Async tasks
    /// Optimal where each Message naturally lends itself to independent processing with no ordering constraints
    static member Start(config : KafkaConsumerConfig, degreeOfParallelism : int) =
        let log = Log.ForContext<Messages>()
        let interpreter, processor = MessageInterpreter(), Processor()
        let handleMessage (KeyValue (streamName,eventsSpan)) = async {
            for x in interpreter.TryDecode(streamName,eventsSpan) do
                processor.Handle x }
        ParallelConsumer.Start(log, config, degreeOfParallelism, handleMessage, statsInterval = TimeSpan.FromSeconds 30., logExternalStats = processor.DumpStats)
        
type Streams =
    static member Start(config : KafkaConsumerConfig, degreeOfParallelism : int) =
        let log = Log.ForContext<Streams>()
        let interpreter, processor = MessageInterpreter(), Processor()
        let statsInterval, stateInterval = TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 5.
        let handle (streamName : string, span : Propulsion.Streams.StreamSpan<_>) = async {
            for x in interpreter.Interpret(streamName, EquinoxSpan.EnumCodecEvents span) do
                processor.Handle x
            return span.events.Length }
        let categorize (streamName : string) =
            streamName.Split([|'-';'_'|],2).[0]
        StreamsConsumer.Start
            (   log, config, degreeOfParallelism, interpreter.EnumStreamEvents, handle, categorize, maxSubmissionsPerPartition = 4,
                statsInterval = statsInterval, stateInterval = stateInterval, logExternalStats = processor.DumpStats)
        
type BatchesAsync =
    /// Starts a consumer that consumes a topic in a batched mode, based on a source defined by `cfg`
    /// Processing fans out as parallel Async computations (limited to max `degreeOfParallelism` concurrent tasks
    /// The messages in the batch emanate from a single partition and are all in sequence
    /// notably useful where there's an ability to share some processing cost across a batch of work by doing the processing in phases
    static member Start(config : KafkaConsumerConfig, degreeOfParallelism : int) =
        let log = Log.ForContext<BatchesAsync>()
        let dop = new SemaphoreSlim(degreeOfParallelism)
        let interpreter = MessageInterpreter()
        let handleBatch (msgs : ConsumeResult<_,_>[]) = async {
            let processor = Processor()
            let! _ =
                seq { for x in msgs do yield! interpreter.TryDecode(x.Key, x.Value) }
                |> Seq.map (fun x -> async { processor.Handle x } |> dop.Throttle)
                |> Async.Parallel
            processor.DumpStats log }
        BatchedConsumer.Start(log, config, handleBatch)