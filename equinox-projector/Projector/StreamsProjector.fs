module Jet.Projection.Kafka

open Confluent.Kafka
open Jet.ConfluentKafka.FSharp
open Jet.Projection
open Jet.Projection.Ingestion // AwaitTaskCorrect
open Newtonsoft.Json
open Serilog
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type PipelinedProjector private (task : Task<unit>, triggerStop, startIngester) =

    interface IDisposable with member __.Dispose() = __.Stop()

    member __.StartIngester(rangeLog : ILogger) = startIngester rangeLog

    /// Inspects current status of processing task
    member __.Status = task.Status

    /// Request cancellation of processing
    member __.Stop() = triggerStop ()

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

    static member Start(log : Serilog.ILogger, pumpDispatcher, pumpScheduler, pumpSubmitter, startIngester) =
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

            // await for either handler-driven abend or external cancellation via Stop()
            do! Async.AwaitTaskCorrect tcs.Task }
        let task = Async.StartAsTask machine
        let triggerStop () =
            log.Information("Stopping")
            cts.Cancel();  
        new PipelinedProjector(task, triggerStop, startIngester)

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

    type PipelinedStreamsProjector =
        static member Start(log : Serilog.ILogger, pumpDispatcher, pumpScheduler, maxReadAhead, submitStreamsBatch, statsInterval, ?maxSubmissionsPerPartition) =
            let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5
            let mapBatch onCompletion (x : Submission.Batch<StreamItem>) : StreamScheduling.StreamsBatch =
                let onCompletion () = x.onCompletion(); onCompletion()
                StreamScheduling.StreamsBatch.Create(onCompletion, x.messages) |> fst
            let submitBatch (x : StreamScheduling.StreamsBatch) : int =
                submitStreamsBatch x
                x.RemainingStreamsCount
            let tryCompactQueue (queue : Queue<StreamScheduling.StreamsBatch>) =
                let mutable acc, worked = None, false
                for x in queue do
                    match acc with
                    | None -> acc <- Some x
                    | Some a -> if a.TryMerge x then worked <- true
                worked
            let submitter = Submission.SubmissionEngine<_,_>(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, tryCompactQueue=tryCompactQueue)
            let startIngester rangeLog = Jet.Projection.Ingestion.StreamsIngester.Start(rangeLog, maxReadAhead, submitter.Ingest)
            PipelinedProjector.Start(log, pumpDispatcher, pumpScheduler, submitter.Pump(), startIngester)

type StreamsProjector =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, project, categorize, ?statsInterval, ?stateInterval)
            : PipelinedProjector =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionStats = StreamScheduling.Stats<_,_>(log.ForContext<StreamScheduling.Stats<_,_>>(), statsInterval, stateInterval)
        let dispatcher = StreamScheduling.Dispatcher<_>(maxConcurrentStreams)
        let streamScheduler = StreamScheduling.Factory.Create(dispatcher, projectionStats, project, fun s l -> s.Dump(l, categorize))
        PipelinedStreamsProjector.Start(log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval)
        
type KafkaStreamsProjector =
    static member Start(log : ILogger, maxReadAhead, maxConcurrentStreams, clientId, broker, topic, categorize, ?statsInterval, ?stateInterval)
            : PipelinedProjector =
        let producerConfig = KafkaProducerConfig.Create(clientId, broker, Acks.Leader, compression = CompressionType.Lz4, maxInFlight = 1_000_000, linger = TimeSpan.Zero)
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
        let dispatcher = StreamScheduling.Dispatcher<_>(maxConcurrentStreams)
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionAndKafkaStats = Stats(log.ForContext<Stats>(), categorize, statsInterval, stateInterval)
        let streamScheduler = StreamScheduling.StreamSchedulingEngine<_,_>(dispatcher, projectionAndKafkaStats, attemptWrite, interpretWriteResultProgress, fun s l -> s.Dump(l, categorize))
        PipelinedStreamsProjector.Start(log, dispatcher.Pump(), streamScheduler.Pump, maxReadAhead, streamScheduler.Submit, statsInterval)