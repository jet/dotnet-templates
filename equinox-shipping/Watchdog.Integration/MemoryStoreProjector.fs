namespace Shipping.Watchdog.Integration

open System
open System.Threading

/// Manages propagation of a stream of events into an inner Projector
type MemoryStoreProjector<'F, 'B> private (log, inner : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<Propulsion.Streams.StreamEvent<'F> seq, 'B>>) =
    let ingester = inner.StartIngester(log, 0)
    let mutable epoch = -1L
    let mutable completed = None
    let mutable checkpointed = None

    let queue = new System.Collections.Concurrent.BlockingCollection<_>(1024)
    member _.Pump = async {
        for epoch, checkpoint, events, markCompleted in queue.GetConsumingEnumerable() do
            let! _ = ingester.Submit(epoch, checkpoint, events, markCompleted) in ()
    }

    /// Starts a projector loop, feeding into the supplied target
    static member Start(log, target : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<Propulsion.Streams.StreamEvent<'F> seq, 'B>>) =
        let instance = MemoryStoreProjector(log, target)
        do Async.Start(instance.Pump)
        instance

    /// Accepts an individual batch of events from a stream for submission into the <c>inner</c> projector
    member _.Submit(stream, events : FsCodec.ITimelineEvent<'F> seq) =
        let epoch = Interlocked.Increment &epoch
        let events = MemoryStoreLogger.toStreamEvents stream events
        MemoryStoreLogger.renderSubmit log (epoch, stream, events)
        let markCompleted () =
            MemoryStoreLogger.renderCompleted log (epoch, stream)
            Volatile.Write(&completed, Some epoch)
        let checkpoint = async { checkpointed <- Some epoch }
        queue.Add((epoch, checkpoint, events, markCompleted))

    /// Wires specified <c>Observable</c> source (e.g. <c>VolatileStore.Committed</c>) to <c>Submit</c> into the <c>inner</c> projector
    member this.Subscribe(source) =
        Observable.subscribe this.Submit source

    /// Waits until all <c>Submit</c>ted batches have been fed into the <c>inner</c> Projector
    member _.AwaitWithStopOnCancellation
        (   /// sleep time while awaiting completion
            ?delay,
            /// interval at which to log progress of Projector loop
            ?logInterval) = async {
        if -1L = Volatile.Read(&epoch) then
            log.Warning("No events submitted; completing immediately")
        else
            let delay = defaultArg delay TimeSpan.FromMilliseconds 5.
            let maybeLog =
                let logInterval = defaultArg logInterval (TimeSpan.FromSeconds 10.)
                let logDue = Propulsion.Internal.intervalCheck logInterval
                fun () ->
                    if logDue () then
                        log.ForContext("checkpoint", checkpointed)
                            .Information("Waiting for epoch {epoch}. Current completed epoch {completed}", epoch, Option.toNullable completed)
            let delayMs = int delay.TotalMilliseconds
            while Some (Volatile.Read &epoch) <> Volatile.Read &completed do
                maybeLog()
                do! Async.Sleep delayMs
        // the ingestion pump can be stopped now...
        ingester.Stop()
        // as we've validated all submissions have had their processing completed, we can stop the inner projector too
        inner.Stop()
        // trigger termination of GetConsumingEnumerable()-driven pumping loop
        queue.CompleteAdding()
        return! inner.AwaitWithStopOnCancellation()
    }
