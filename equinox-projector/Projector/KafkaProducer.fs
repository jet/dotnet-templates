module Jet.Projection.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Jet.Projection
open Newtonsoft.Json
open Serilog
open System
open System.Collections.Generic

[<AutoOpen>]
module private Impl =
    let inline mb x = float x / 1024. / 1024.
    type OkResult = int64 * (int*int) * DeliveryResult<string,string>
    type FailResult = (int*int) * exn

type Stats(log : ILogger, categorize, statsInterval, statesInterval) =
    inherit StreamScheduling.Stats<OkResult,FailResult>(log, statsInterval, statesInterval)
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
        | StreamScheduling.Added _ -> () // Processed by standard logging already; we have nothing to add
        | StreamScheduling.Result (stream, Choice1Of2 (_,(es,bs),_r)) ->
            adds stream okStreams
            okEvents <- okEvents + es
            okBytes <- okBytes + int64 bs
            incr resultOk
        | StreamScheduling.Result (stream, Choice2Of2 ((es,bs),exn)) ->
            bads stream failStreams
            exnEvents <- exnEvents + es
            exnBytes <- exnBytes + int64 bs
            incr resultExnOther
            log.Warning(exn,"Could not write {b:n0} bytes {e:n0}e in stream {stream}", bs, es, stream)

type KafkaStreamProducer =
    static member Start(log : Serilog.ILogger, clientId, broker, topic, maxInFlightMessages, categorize, ?statsInterval, ?stateInterval, ?maxSubmissionsPerPartition)
            : Submission.SubmissionEngine<StreamItem,StreamScheduling.StreamsBatch> =
        let producerConfig = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, maxInFlight=1_000_000, linger = TimeSpan.Zero)
        let producer = KafkaProducer.Create(log, producerConfig, topic)
        let attemptWrite (_writePos,stream,fullBuffer : Span) = async {
            let maxEvents, maxBytes = 16384, 1_000_000 - (*fudge*)4096
            let ((eventCount,_) as stats), span = Buffering.Span.slice (maxEvents,maxBytes) fullBuffer
            let rendered = Equinox.Projection.Codec.RenderedSpan.ofStreamSpan stream span.index span.events
            try let! res = producer.ProduceAsync(stream,JsonConvert.SerializeObject(rendered))
                return Choice1Of2 (span.index + int64 eventCount,stats,res)
            with e -> return Choice2Of2 (stats,e) }
        let interpretWriteResultProgress _streams _stream = function
            | Choice1Of2 (i',_, _) -> Some i'
            | Choice2Of2 (_,_) -> None
        let dispatcher = StreamScheduling.Dispatcher<_>(maxInFlightMessages)
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionAndKafkaStats = Stats(log.ForContext<Stats>(), categorize, statsInterval, stateInterval)
        let streamScheduler = StreamScheduling.StreamSchedulingEngine<_,_>(dispatcher, projectionAndKafkaStats, attemptWrite, interpretWriteResultProgress, fun s l -> s.Dump(l, categorize))
        // TODO pump
        // TODO delegate to StreamProjectorSink
        let mapBatch onCompletion (x : Submission.Batch<StreamItem>) : StreamScheduling.StreamsBatch =
            let onCompletion () = x.onCompletion(); onCompletion()
            StreamScheduling.StreamsBatch.Create(onCompletion, x.messages) |> fst
        let submitBatch (x : StreamScheduling.StreamsBatch) : int =
            streamScheduler.Submit x
            x.RemainingStreamsCount
        let tryCompactQueue (queue : Queue<StreamScheduling.StreamsBatch>) =
            let mutable acc, worked = None, false
            for x in queue do
                match acc with
                | None -> acc <- Some x
                | Some a -> if a.TryMerge x then worked <- true
            worked
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
        Submission.SubmissionEngine<_,_>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, tryCompactQueue=tryCompactQueue)