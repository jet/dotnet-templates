namespace Shipping.Watchdog.Integration

module MemoryStoreLogger =

    let renderSubmit (log : Serilog.ILogger) (epoch, stream, events : Propulsion.Streams.StreamEvent<'F> array) =
        if log.IsEnabled Serilog.Events.LogEventLevel.Verbose then
            let types = seq { for x in events -> x.event.EventType }
            log.ForContext("types", types).Debug("Submit #{epoch} {stream}x{count}", epoch, stream, events.Length)
        elif log.IsEnabled Serilog.Events.LogEventLevel.Debug then
            let types = seq { for x in events -> x.event.EventType } |> Seq.truncate 5
            log.Debug("Submit #{epoch} {stream}x{count} {types}", epoch, stream, events.Length, types)
    let renderCompleted (log : Serilog.ILogger) (epoch, stream) =
        log.Verbose("Done!  #{epoch} {stream}", epoch, stream)

    let toStreamEvents stream (events : FsCodec.ITimelineEvent<'F> seq) =
        [| for x in events -> { stream = stream; event = x } : Propulsion.Streams.StreamEvent<'F> |]

    /// Wires specified <c>Observable</c> source (e.g. <c>VolatileStore.Committed</c>) to the Logger
    let subscribe log source =
        let mutable epoch = -1L
        let aux (stream, events) =
            let events = toStreamEvents stream events
            let epoch = System.Threading.Interlocked.Increment &epoch
            renderSubmit log (epoch, stream, events)
        if log.IsEnabled Serilog.Events.LogEventLevel.Debug then Observable.subscribe aux source
        else { new System.IDisposable with member _.Dispose() = () }

/// Holds Equinox MemoryStore. Disposable to correctly manage unsubscription of logger at end of test
type MemoryStoreFixture() =
    let store = Equinox.MemoryStore.VolatileStore<byte[]>()
    let mutable disconnectLog : (unit -> unit) option = None
    member val Config = Shipping.Domain.Config.Store.Memory store
    member _.TestOutput with set testOutput =
        if Option.isSome disconnectLog then invalidOp "Cannot connect more than one test output"
        let log = XunitLogger.forTest testOutput
        disconnectLog <- Some (MemoryStoreLogger.subscribe log store.Committed).Dispose
    interface System.IDisposable with member _.Dispose() = match disconnectLog with Some f -> f () | None -> ()
