module Equinox.Projection.Cosmos.Metrics

// TODO move into equinox.cosmos

open System

module RuCounters =
    open Equinox.Cosmos.Store
    open Serilog.Events

    let inline (|Stats|) ({ interval = i; ru = ru }: Log.Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds

    let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|) = function
        | Log.Tip (Stats s)
        | Log.TipNotFound (Stats s)
        | Log.TipNotModified (Stats s)
        | Log.Query (_,_, (Stats s)) -> CosmosReadRc s
        // slices are rolled up into batches so be sure not to double-count
        | Log.Response (_,(Stats s)) -> CosmosResponseRc s
        | Log.SyncSuccess (Stats s)
        | Log.SyncConflict (Stats s) -> CosmosWriteRc s
        | Log.SyncResync (Stats s) -> CosmosResyncRc s
    let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|CosmosMetric|_|) (logEvent : LogEvent) : Log.Event option =
        match logEvent.Properties.TryGetValue("cosmosEvt") with
        | true, SerilogScalar (:? Log.Event as e) -> Some e
        | _ -> None
    type RuCounter =
        { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
        static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
        member __.Ingest (ru, ms) =
            System.Threading.Interlocked.Increment(&__.count) |> ignore
            System.Threading.Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
            System.Threading.Interlocked.Add(&__.ms, ms) |> ignore
    type RuCounterSink() =
        static member val Read = RuCounter.Create() with get, set
        static member val Write = RuCounter.Create() with get, set
        static member val Resync = RuCounter.Create() with get, set
        static member Reset() =
            RuCounterSink.Read <- RuCounter.Create()
            RuCounterSink.Write <- RuCounter.Create()
            RuCounterSink.Resync <- RuCounter.Create()
        interface Serilog.Core.ILogEventSink with
            member __.Emit logEvent = logEvent |> function
                | CosmosMetric (CosmosReadRc stats) -> RuCounterSink.Read.Ingest stats
                | CosmosMetric (CosmosWriteRc stats) -> RuCounterSink.Write.Ingest stats
                | CosmosMetric (CosmosResyncRc stats) -> RuCounterSink.Resync.Ingest stats
                | _ -> ()

let dumpRuStats duration (log: Serilog.ILogger) =
    let stats =
      [ "Read", RuCounters.RuCounterSink.Read
        "Write", RuCounters.RuCounterSink.Write
        "Resync", RuCounters.RuCounterSink.Resync ]
    let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
    let logActivity name count rc lat =
        if count <> 0L then
            log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
    for name, stat in stats do
        let ru = float stat.rux100 / 100.
        totalCount <- totalCount + stat.count
        totalRc <- totalRc + ru
        totalMs <- totalMs + stat.ms
        logActivity name stat.count ru stat.ms
    logActivity "TOTAL" totalCount totalRc totalMs
    // Yes, there's a minor race here!
    RuCounters.RuCounterSink.Reset()
    let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
    let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
    for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d) 