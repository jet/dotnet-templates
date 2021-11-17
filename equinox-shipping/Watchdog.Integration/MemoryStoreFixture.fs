namespace Shipping.Watchdog.Integration

module MemoryStoreLogger =

    let private propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : Serilog.ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

    let private propEventJsonUtf8 name (events : Propulsion.Streams.StreamEvent<byte[]> array) (log : Serilog.ILogger) =
        log |> propEvents name (seq {
            for { event = e } in events do
                match e.Data with
                | null -> ()
                | d -> System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString d) })

    let renderSubmit (log : Serilog.ILogger) (epoch, stream, events : Propulsion.Streams.StreamEvent<'F> array) =
        if log.IsEnabled Serilog.Events.LogEventLevel.Verbose then
            let log =
                if (not << log.IsEnabled) Serilog.Events.LogEventLevel.Debug then log
                elif typedefof<'F> <> typeof<byte[]> then log
                else log |> propEventJsonUtf8 "Json" (unbox events)
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
