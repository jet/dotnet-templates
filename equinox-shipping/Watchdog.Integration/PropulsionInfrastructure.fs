[<AutoOpen>]
module Shipping.Watchdog.Integration.PropulsionInfrastructure

open System
open System.Threading

type Async with

    /// Returns an async computation which runs the argument computation but raises an exception if it doesn't complete
    /// by the specified timeout.
    static member timeoutAfter (timeout : System.TimeSpan) (c : Async<'a>) = async {
        let! r = Async.StartChild(c, int timeout.TotalMilliseconds)
        return! r }

/// Manages propagation of a stream of events into an inner Projector
type MemoryStoreProjector<'F, 'B> private (log, inner : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<Propulsion.Streams.StreamEvent<'F> seq, 'B>>) =
    let ingester = inner.StartIngester(log, 0)
    let mutable epoch = -1L
    let mutable completed = None
    let mutable checkpointed = None

    let queue = new System.Collections.Concurrent.BlockingCollection<_>(1024)
    member __.Pump = async {
        for epoch, checkpoint, events, markCompleted in queue.GetConsumingEnumerable() do
            let! _ = ingester.Submit(epoch, checkpoint, events, markCompleted) in ()
    }

    /// Starts a projector loop, feeding into the supplied target
    static member Start(log, target : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<Propulsion.Streams.StreamEvent<'F> seq, 'B>>) =
        let instance = MemoryStoreProjector(log, target)
        do Async.Start(instance.Pump)
        instance

    /// Accepts an individual batch of events from a stream for submission into the <c>inner</c> projector
    member __.Submit(stream, events : 'E seq) =
        let epoch = Interlocked.Increment &epoch
        log.Debug("Submitted! {stream} {epoch}", stream, epoch)
        let markCompleted () =
            log.Debug("Completed! {stream} {epoch}", stream, epoch)
            Volatile.Write(&completed, Some epoch)
        let checkpoint = async { checkpointed <- Some epoch }
        queue.Add((epoch, checkpoint, seq { for x in events -> { stream = stream; event = x } }, markCompleted))

    /// Wires specified <c>Observable</c> source (e.g. <c>VolatileStore.Committed</c>) to <c>Submit</c> into the <c>inner</c> projector
    member this.Subscribe(source) =
        Observable.subscribe this.Submit source

    /// Waits until all <c>Submit</c>ted batches have been fed into the <c>inner</c> Projector
    member __.AwaitCompletion
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
                            .Information("Waiting for epoch {epoch}. Current completed epoch {completed}", epoch, completed)
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
        return! inner.AwaitCompletion()
    }

type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let template = "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message} {Properties}{NewLine}{Exception}"
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter(template, null)
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        writer |> string |> testOutput.WriteLine
        writer |> string |> System.Diagnostics.Debug.Write
    interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

module TestOutputLogger =

    open Serilog

    let create output =
        let logger = TestOutputAdapter output
        LoggerConfiguration()
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
            .WriteTo.Sink(logger)
            .CreateLogger()
