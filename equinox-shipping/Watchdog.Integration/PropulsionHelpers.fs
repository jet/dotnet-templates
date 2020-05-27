[<AutoOpen>]
module Shipping.Watchdog.Integration.PropulsionHelpers

open System
open System.Threading

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
