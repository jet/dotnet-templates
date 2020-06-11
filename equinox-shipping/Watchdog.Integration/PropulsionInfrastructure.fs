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

type MemoryStoreSource<'F, 'B>(sink : Propulsion.ProjectorPipeline<Propulsion.Ingestion.Ingester<Propulsion.Streams.StreamEvent<'F> seq, 'B>>) =
    let ingester = sink.StartIngester(Serilog.Log.Logger, 0)
    let mutable epoch = -1L
    let mutable completed = None
    let mutable checkpointed = None

    member __.Submit(stream, events : 'E seq) =
        let epoch = Interlocked.Increment &epoch
        let markCompleted () = completed <- Some epoch
        let checkpoint = async { checkpointed <- Some epoch }
        ingester.Submit(epoch, checkpoint, seq { for x in events -> { stream = stream; event = x } }, markCompleted)
        |> Async.Ignore
        |> Async.RunSynchronously
        // TODO use a ProducerConsumerCollection of some kind

    member __.AwaitCompletion(?delay, ?logInterval, ?log) = async {
        let delay = defaultArg delay TimeSpan.FromMilliseconds 5.
        let maybeLog =
            let logInterval = defaultArg logInterval (TimeSpan.FromMinutes 1.)
            let logDue = Propulsion.Internal.intervalCheck logInterval
            let log = defaultArg log Serilog.Log.Logger
            fun () ->
                if logDue () then
                    log.Information("Waiting for epoch {epoch} (completed to {completed}, checkpointed to {checkpoint})",
                        epoch, completed, checkpointed)
        let delayMs = int delay.TotalMilliseconds
        while Some (Volatile.Read &epoch) <> Volatile.Read &completed do
            maybeLog()
            do! Async.Sleep delayMs
        sink.Stop()
        return! sink.AwaitCompletion()
    }

type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null)
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
        LoggerConfiguration().Destructure.FSharpTypes().WriteTo.Sink(logger).CreateLogger()

