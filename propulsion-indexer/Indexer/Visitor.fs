module IndexerTemplate.Indexer.Visitor

type Outcome = (struct (string * (struct (string * System.TimeSpan))[] * (string * int)[]))
module Outcome =
    let private eventType (x: Propulsion.Sinks.Event) = x.EventType
    let private eventCounts = Array.countBy eventType
    let private create sn ham spam: Outcome = struct (FsCodec.StreamName.Category.ofStreamName sn, ham, spam)
    let render sn ham spam = create sn ham (eventCounts spam)
    let render_ sn ham spam elapsedS =
        let share = TimeSpan.seconds (match Array.length ham with 0 -> 0 | count -> elapsedS / float count)
        create sn (ham |> Array.map (fun x -> struct (eventType x, share))) (eventCounts spam)

[<AbstractClass>]
type StatsBase<'outcome>(log, statsInterval, stateInterval, verboseStore, ?abendThreshold) =
    inherit Propulsion.Streams.Stats<'outcome>(log, statsInterval, stateInterval, failThreshold = TimeSpan.seconds 120, ?abendThreshold = abendThreshold)

    override _.DumpStats() =
        base.DumpStats()
        Equinox.CosmosStore.Core.Log.InternalMetrics.dump Serilog.Log.Logger

    override _.Classify(e) =
        match e with
        | OutcomeKind.StoreExceptions kind -> kind
        // Cosmos Emulator overload manifests as 'Response status code does not indicate success: ServiceUnavailable (503); Substatus: 20002'
        // (in verbose mode, we let the actual exception bubble up)
        | Equinox.CosmosStore.Exceptions.ServiceUnavailable when not verboseStore -> Propulsion.Streams.OutcomeKind.Tagged "cosmos503"
        | :? System.TimeoutException -> Propulsion.Streams.OutcomeKind.Tagged "timeoutEx"
        // Emulator can emit this (normally only during laptop sleeps)
        | Equinox.CosmosStore.Exceptions.CosmosStatus System.Net.HttpStatusCode.Forbidden as e
            when e.Message.Contains "Authorization token is not valid at the current time. Please provide a valid token" ->
            Propulsion.Streams.OutcomeKind.Timeout
        | x ->
            base.Classify x
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

type CategoryCounters() =
    let cats = System.Collections.Generic.Dictionary<string, Propulsion.Internal.Stats.Counters>()
    member _.Ingest(category, counts) =
        let cat =
            match cats.TryGetValue category with
            | false, _ -> let acc = Propulsion.Internal.Stats.Counters() in cats.Add(category, acc); acc
            | true, acc -> acc
        for event, count : int in counts do cat.Ingest(event, count)
    member _.Categories = cats.Keys
    member _.StatsDescending cat =
        match cats.TryGetValue cat with
        | true, acc -> acc.StatsDescending
        | false, _ -> Seq.empty
    member _.DumpGrouped(log: Serilog.ILogger, totalLabel) =
        if cats.Count <> 0 then
            Propulsion.Internal.Stats.dumpCounterSet log totalLabel cats
    member _.Clear() = cats.Clear()

type Stats(log, statsInterval, stateInterval, verboseStore, abendThreshold) =
    inherit StatsBase<Outcome>(log, statsInterval, stateInterval, verboseStore, abendThreshold = abendThreshold)
    let mutable handled, ignored = 0, 0
    let accHam, accSpam = CategoryCounters(), CategoryCounters()
    override _.HandleOk((category, ham, spam)) =
        accHam.Ingest(category, ham |> Seq.countBy Propulsion.Internal.ValueTuple.fst)
        accSpam.Ingest(category, spam)
        handled <- handled + Array.length ham
        ignored <- ignored + Array.sumBy snd spam
    override _.DumpStats() =
        if handled > 0 || ignored > 0 then
            if ignored > 0 then log.Information(" Handled {count}, skipped {skipped}", handled, ignored)
            handled <- 0; ignored <- 0
    override _.DumpState purge =
        for cat in Seq.append accHam.Categories accSpam.Categories |> Seq.distinct |> Seq.sort do
            let ham, spam = accHam.StatsDescending(cat) |> Array.ofSeq, accSpam.StatsDescending cat |> Array.ofSeq
            if ham.Length > 00 then log.Information(" Category {cat} handled {@ham}", cat, ham)
            if spam.Length <> 0 then log.Information(" Category {cat} ignored {@spam}", cat, spam)
        if purge then
            accHam.Clear(); accSpam.Clear()

let private handle isValidEvent stream (events: Propulsion.Sinks.Event[]): Async<_ * Outcome> = async {
    let ham, spam = events |> Array.partition isValidEvent
    return Propulsion.Sinks.StreamResult.AllProcessed, Outcome.render_ stream ham spam 0 }

module Factory =

    let createHandler = handle
