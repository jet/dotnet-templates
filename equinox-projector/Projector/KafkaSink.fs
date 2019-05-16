module ProjectorTemplate.KafkaSink

open Confluent.Kafka
open Equinox.Projection2
open Equinox.Projection.Codec
open Equinox.Store
open Jet.ConfluentKafka.FSharp
open Newtonsoft.Json
open System

open Serilog
open System.Threading.Tasks
open System.Threading
type KafkaProducer private (log: ILogger, inner : IProducer<string, string>, topic : string) =
    member __.Topic = topic

    interface IDisposable with member __.Dispose() = inner.Dispose()

    /// Produces a single item, yielding a response upon completion/failure of the ack
    /// <remarks>
    ///     There's no assurance of ordering [without dropping `maxInFlight` down to `1` and annihilating throughput].
    ///     Thus its critical to ensure you don't submit another message for the same key until you've had a success / failure response from the call.<remarks/>
    member __.ProduceAsync(key, value) : Async<DeliveryResult<_,_>>= async {
        return! inner.ProduceAsync(topic, Message<_,_>(Key=key, Value=value)) |> Async.AwaitTaskCorrect }

    /// Produces a batch of supplied key/value messages. Results are returned in order of writing (which may vary from order of submission).
    /// <throws>
    ///    1. if there is an immediate local config issue
    ///    2. upon receipt of the first failed `DeliveryReport` (NB without waiting for any further reports) </throws>
    /// <remarks>
    ///    Note that the delivery and/or write order may vary from the supplied order (unless you drop `maxInFlight` down to 1, massively constraining throughput).
    ///    Thus it's important to note that supplying >1 item into the queue bearing the same key risks them being written to the topic out of order. <remarks/>
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

    static member Create(log : ILogger, config : KafkaProducerConfig, topic : string) =
        if String.IsNullOrEmpty topic then nullArg "topic"
        log.Information("Producing... {broker} / {topic} compression={compression} acks={acks}", config.Broker, topic, config.Compression, config.Acks)
        let producer =
            ProducerBuilder<string, string>(config.Kvps)
                .SetLogHandler(fun _p m -> log.Information("{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
                .SetErrorHandler(fun _p e -> log.Error("{reason} code={code} isBrokerError={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
                .Build()
        new KafkaProducer(log, producer, topic)

module Codec =
    /// Rendition of an event when being projected as Spans to Kafka
    type [<NoEquality; NoComparison>] RenderedSpanEvent =
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

    /// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
    type [<NoEquality; NoComparison>]
        RenderedSpan =
        {   /// base 'i' value for the Events held herein
            i: int64

            /// The Events comprising this span
            e: RenderedSpanEvent[] }

    module RenderedSpan =
        let ofStreamSpan (x: Buffer.StreamSpan) : RenderedSpan =
            { i = x.span.index; e = [| for x in x.span.events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta }|] }
            
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
            let rendered = Codec.RenderedSpan.ofStreamSpan trimmed
            try let! res = producer.ProduceAsync(trimmed.stream,JsonConvert.SerializeObject(rendered))
                return Choice1Of2 (trimmed.span.index + int64 eventCount,stats,res)
            with e -> return Choice2Of2 (stats,e) }
        let interpretWriteResultProgress _streams _stream = function
            | Choice1Of2 (i',_, _) -> Some i'
            | Choice2Of2 (_,_) -> None
        let projectionAndKafkaStats = Stats(log.ForContext<Stats>(), categorize, statsInterval, statesInterval)
        Engine<_,_>.Start(projectionAndKafkaStats, maxInFlightMessages, attemptWrite, interpretWriteResultProgress, fun s l -> s.Dump(l, categorize))