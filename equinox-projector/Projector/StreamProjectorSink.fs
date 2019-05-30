namespace ProjectorTemplate.Projector

open Jet.Projection
open System
open System.Collections.Generic

type StreamProjectorSink =
    static member Start(log : Serilog.ILogger, project, maxConcurrentStreams, categorize, ?statsInterval, ?stateInterval, ?maxSubmissionsPerPartition)
            : Submission.SubmissionEngine<StreamItem,StreamScheduling.StreamsBatch> =
        let statsInterval, stateInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.), defaultArg stateInterval (TimeSpan.FromMinutes 5.)
        let projectionStats = StreamScheduling.Stats<_,_>(log.ForContext<StreamScheduling.Stats<_,_>>(), statsInterval, stateInterval)
        let dispatcher = StreamScheduling.Dispatcher<_>(maxConcurrentStreams)
        let streamScheduler = StreamScheduling.Factory.Create(dispatcher, projectionStats, project, fun s l -> s.Dump(l, categorize))
        // TODO pump
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