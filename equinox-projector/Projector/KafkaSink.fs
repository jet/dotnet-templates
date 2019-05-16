module ProjectorTemplate.Projector.KafkaSink

open Confluent.Kafka
open Equinox.Projection2
open Equinox.Projection.Codec
open Equinox.Store
open Jet.ConfluentKafka.FSharp
open Newtonsoft.Json
open System

module Codec =
    /// Rendition of an event within a Span
    type [<NoEquality; NoComparison>] RenderedEvent =
        {   /// Event Type associated with event data in `d`
            c: string

            /// Timestamp of original write
            t: DateTimeOffset // ISO 8601

            /// Event body, as UTF-8 encoded json ready to be injected directly into the Json being rendered
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata, as UTF-8 encoded json, ready to emit directly (entire field is not written if value is null)
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] }

    /// Rendition of a continguous span of events for a given stream
    type [<NoEquality; NoComparison>] RenderedSpan =
        {   /// Stream Name
            s: string

            /// base 'i' value for the Events held herein, reflecting the index associated with the first event in the span
            i: int64

            /// The Events comprising this span
            e: RenderedEvent[] }

    /// Helpers for mapping to/from `Equinox.Codec` types
    module RenderedSpan =
        let ofStreamSpan (stream : string) (index : int64) (events : seq<Equinox.Codec.IEvent<byte[]>>) : RenderedSpan =
            {   s = stream
                i = index
                e = [| for x in events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta } |] }
        let enumEvents (span : RenderedSpan) : seq<Equinox.Codec.IEvent<byte[]>> = seq {
            for x in span.e ->
                Equinox.Codec.Core.EventData.Create(x.c, x.d, x.m, timestamp=x.t) }

open Equinox.Cosmos.Core
open Equinox.Cosmos.Store
open Equinox.Projection
open Equinox.Projection2
open Equinox.Projection2.Buffer
open Equinox.Projection2.Scheduling
open Serilog
open System.Threading
open System.Collections.Generic

[<AutoOpen>]
module private Impl =
    let inline mb x = float x / 1024. / 1024.
    type OkResult = int64 * (int*int) * DeliveryResult<string,string>
    type FailResult = (int*int) * exn

type Stats(log : ILogger, categorize, statsInterval, statesInterval) =
    inherit Stats<OkResult,FailResult>(log, statsInterval, statesInterval)
    let okStreams, failStreams, badCats, resultOk, resultExnOther = HashSet(), HashSet(), CatStats(), ref 0, ref 0
    let mutable okEvents, okBytes, exnEvents, exnBytes = 0, 0L, 0, 0L

    override __.DumpExtraStats() =
        log.Information("Completed {mb:n0}MB {completed:n0}r {streams:n0}s {events:n0}e ({ok:n0} ok)", mb okBytes, !resultOk, okStreams.Count, okEvents, !resultOk)
        okStreams.Clear(); resultOk := 0; okEvents <- 0; okBytes <- 0L
        if !resultExnOther <> 0 then
            log.Warning("Exceptions {mb:n0}MB {fails:n0}r {streams:n0}s {events:n0}e", mb exnBytes, !resultExnOther, failStreams.Count, exnEvents)
            resultExnOther := 0; failStreams.Clear(); exnBytes <- 0L; exnEvents <- 0
            log.Warning("Malformed cats {@badCats}", badCats.StatsDescending)
            badCats.Clear()

    override __.Handle message =
        let inline adds x (set:HashSet<_>) = set.Add x |> ignore
        let inline bads x (set:HashSet<_>) = badCats.Ingest(categorize x); adds x set
        base.Handle message
        match message with
        | Merge _ | Added _ -> () // Processed by standard logging already; we have nothing to add
        | Result (stream, Choice1Of2 (_,(es,bs),_r)) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            incr resultOk
        | Result (stream, Choice2Of2 ((es,bs),exn)) ->
            bads stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            incr resultExnOther
            log.Warning(exn,"Could not write {b:n0} bytes {e:n0}e in stream {stream}", bs, es, stream)

type Scheduler =
    static member Start(log : Serilog.ILogger, clientId, broker, topic, maxInFlightMessages, categorize, (statsInterval, statesInterval))
            : Scheduling.Engine<OkResult,FailResult> =
        let producerConfig = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, maxInFlight=1_000_000)
        let producer = KafkaProducer.Create(Log.Logger, producerConfig, topic)
        let attemptWrite (_writePos,fullBuffer) = async {
            let maxEvents, maxBytes = 16384, 1_000_000 - (*fudge*)4096
            let ((eventCount,_) as stats), span' = Span.slice (maxEvents,maxBytes) fullBuffer.span
            let trimmed = { fullBuffer with span = span' }
            let rendered = Codec.RenderedSpan.ofStreamSpan trimmed.stream trimmed.span.index trimmed.span.events
            try let! res = producer.ProduceAsync(trimmed.stream,JsonConvert.SerializeObject(rendered))
                return Choice1Of2 (trimmed.span.index + int64 eventCount,stats,res)
            with e -> return Choice2Of2 (stats,e) }
        let interpretWriteResultProgress _streams _stream = function
            | Choice1Of2 (i',_, _) -> Some i'
            | Choice2Of2 (_,_) -> None
        let projectionAndKafkaStats = Stats(log.ForContext<Stats>(), categorize, statsInterval, statesInterval)
        Engine<_,_>.Start(projectionAndKafkaStats, maxInFlightMessages, attemptWrite, interpretWriteResultProgress, fun s l -> s.Dump(l, categorize))