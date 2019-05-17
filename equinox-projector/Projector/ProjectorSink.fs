module ProjectorTemplate.Projector.ProjectorSink

open Equinox.Projection
open System

type Scheduler =

    static member Start(log, projectorDop, project : Buffer.StreamSpan -> Async<int>, categorize, ?statsInterval, ?statesInterval) =
        let project (_maybeWritePos, batch) = async {
            try let! count = project batch
                return Choice1Of2 (batch.span.index + int64 count)
            with e -> return Choice2Of2 e }
        let interpretProgress _streams _stream = function
            | Choice1Of2 index -> Some index
            | Choice2Of2 _ -> None
        let stats = Scheduling.Stats(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg statesInterval (TimeSpan.FromMinutes 5.))
        let dumpStreams (streams: Scheduling.StreamStates) log = streams.Dump(log, categorize)
        Scheduling.Engine<int64,_>.Start(stats, projectorDop, project, interpretProgress, dumpStreams)

type Ingester =

    /// Starts an Ingester that will submit up to `maxSubmissions` items at a time to the `scheduler`, blocking on Submits when more than `maxRead` batches have yet to complete processing 
    static member Start<'R,'E>(log, scheduler, maxRead, maxSubmissions, categorize, ?statsInterval) : IIngester<int64,StreamItem> =
        let singleSeriesIndex = 0
        let instance = Ingestion.Engine.Start(log, scheduler, maxRead, maxSubmissions, singleSeriesIndex, categorize, statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 1.))
        { new IIngester<int64,StreamItem> with
            member __.Submit(epoch, markCompleted, items) : Async<int*int> =
                instance.Submit(Ingestion.Message.Batch(singleSeriesIndex, epoch, markCompleted, items))
            member __.Stop() = __.Stop() } 