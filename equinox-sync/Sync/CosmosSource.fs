module SyncTemplate.CosmosSource

open Equinox.Cosmos.Projection
open Equinox.Projection
open Equinox.Store // AwaitTaskCorrect
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open System
open System.Collections.Generic

let createRangeSyncHandler (log:ILogger) (createIngester : ILogger -> IIngester<_,_>) (transform : Document -> StreamItem seq) () =
    let mutable rangeIngester = Unchecked.defaultof<_>
    let init rangeLog = async { rangeIngester <- createIngester rangeLog }
    let ingest epoch checkpoint docs = let events = docs |> Seq.collect transform in rangeIngester.Submit(epoch, checkpoint, events)
    let dispose () = rangeIngester.Stop ()
    let sw = System.Diagnostics.Stopwatch() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let processBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let epoch = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64
        let! pt, (cur,max) = ingest epoch (ctx.Checkpoint()) docs |> Stopwatch.Time
        let age, readS, postS = DateTime.UtcNow - docs.[docs.Count-1].Timestamp, float sw.ElapsedMilliseconds / 1000., let e = pt.Elapsed in e.TotalSeconds
        log.Information("Read {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Ingest {pt:f3}s {cur}/{max}",
            epoch, age, docs.Count, ctx.FeedResponse.RequestCharge, readS, postS, cur, max)
        sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
    }
    ChangeFeedObserver.Create(log, processBatch, assign=init, dispose=dispose)

let run (log : ILogger) (sourceDiscovery, source) (auxDiscovery, aux) connectionPolicy (leaseId, startFromTail, maybeLimitDocuments, lagReportFreq : TimeSpan option)
        createRangeProjector (scheduler : ISchedulingEngine) = async {
    let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
        log.Information("Backlog {backlog:n0} (by range: {@rangeLags})", remainingWork |> Seq.map snd |> Seq.sum, remainingWork |> Seq.sortBy fst)
        return! Async.Sleep interval }
    let maybeLogLag = lagReportFreq |> Option.map logLag
    let! _feedEventHost =
        ChangeFeedProcessor.Start
          ( log, sourceDiscovery, connectionPolicy, source, aux, auxDiscovery = auxDiscovery, leasePrefix = leaseId, startFromTail = startFromTail,
            createObserver = createRangeProjector scheduler, ?reportLagAndAwaitNextEstimation = maybeLogLag, ?maxDocuments = maybeLimitDocuments,
            leaseAcquireInterval = TimeSpan.FromSeconds 5., leaseRenewInterval = TimeSpan.FromSeconds 5., leaseTtl = TimeSpan.FromSeconds 10.)
    do! Async.AwaitKeyboardInterrupt()
    scheduler.Stop() }

//#if marveleqx
[<RequireQualifiedAccess>]
module EventV0Parser =
    open Newtonsoft.Json

    /// A single Domain Event as Written by internal Equinox versions
    type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
        EventV0 =
        {   /// DocDb-mandated Partition Key, must be maintained within the document
            s: string // "{streamName}"

            /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
            c: DateTimeOffset // ISO 8601

            /// The Case (Event Type); used to drive deserialization
            t: string // required

            /// 'i' value for the Event
            i: int64 // {index}

            /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            d: byte[] }

    type Document with
        member document.Cast<'T>() =
            let tmp = new Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")

    /// Maps fields in an Equinox V0 Event within an Eqinox.Cosmos Event (in a Batch or Tip) to the interface defined by the default Codec
    let (|StandardCodecEvent|) (x: EventV0) = Equinox.Codec.Core.EventData.Create(x.t, x.d, timestamp = x.c)

    /// We assume all Documents represent Events laid out as above
    let parse (d : Document) : StreamItem =
        let (StandardCodecEvent e) as x = d.Cast<EventV0>()
        { stream = x.s; index = x.i; event = e } : StreamItem

let transformV0 categorize catFilter (v0SchemaDocument: Document) : StreamItem seq = seq {
    let parsed = EventV0Parser.parse v0SchemaDocument
    let streamName = (*if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+*)parsed.stream
    if catFilter (categorize streamName) then
        yield parsed }
//#else
let transformOrFilter categorize catFilter (changeFeedDocument: Document) : StreamItem seq = seq {
    for e in DocumentParser.enumEvents changeFeedDocument do
        // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
        if catFilter (categorize e.stream) then
            yield e
}
//#endif