﻿module SyncTemplate.CosmosSource

open Equinox.Cosmos.Core
open Equinox.Cosmos.Projection
open Equinox.Projection.Engine
open Equinox.Projection.State
open Equinox.Store // AwaitTaskCorrect
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open System
open System.Collections.Generic

let createRangeSyncHandler (log:ILogger) maxPendingBatches (cosmosContext: CosmosContext, maxWriters) (transform : Document -> StreamItem seq) =
    let ingestionEngine = Equinox.Cosmos.Projection.Ingestion.startIngestionEngine (log, maxPendingBatches, cosmosContext, maxWriters, TimeSpan.FromMinutes 1.)
    let mutable trancheEngine = Unchecked.defaultof<_>
    let init rangeLog =
        trancheEngine <- Equinox.Projection.Engine.TrancheEngine.Start (rangeLog, ingestionEngine, maxPendingBatches, maxWriters, TimeSpan.FromMinutes 1.)
    let ingest epoch checkpoint docs =
        let events = docs |> Seq.collect transform |> Array.ofSeq
        trancheEngine.Submit(epoch, checkpoint, events)
    let dispose () = trancheEngine.Stop ()
    let sw = System.Diagnostics.Stopwatch() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let processBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let epoch = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64
        // Pass along the function that the coordinator will run to checkpoint past this batch when such progress has been achieved
        let checkpoint = async { do! ctx.CheckpointAsync() |> Async.AwaitTaskCorrect }
        let! pt, (cur,max) = ingest epoch checkpoint docs |> Stopwatch.Time
        log.Information("Read -{token,6} {count,4} docs {requestCharge,6}RU {l:n1}s Post {pt:n3}s {cur}/{max}",
            epoch, docs.Count, (let c = ctx.FeedResponse.RequestCharge in c.ToString("n1")), float sw.ElapsedMilliseconds / 1000.,
            let e = pt.Elapsed in e.TotalSeconds, cur, max)
        sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
    }
    ChangeFeedObserver.Create(log, processBatch, assign=init, dispose=dispose)

let run (sourceDiscovery, source) (auxDiscovery, aux) connectionPolicy (leaseId, forceSkip, batchSize, lagReportFreq : TimeSpan option)
        createRangeProjector = async {
    let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
        Log.Information("Lags {@rangeLags} (Range, Docs count)", remainingWork)
        return! Async.Sleep interval }
    let maybeLogLag = lagReportFreq |> Option.map logLag
    let! _feedEventHost =
        ChangeFeedProcessor.Start
          ( Log.Logger, sourceDiscovery, connectionPolicy, source, aux, auxDiscovery = auxDiscovery, leasePrefix = leaseId, forceSkipExistingEvents = forceSkip,
            cfBatchSize = batchSize, createObserver = createRangeProjector, ?reportLagAndAwaitNextEstimation = maybeLogLag)
    do! Async.AwaitKeyboardInterrupt() }

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
    type IEvent =
        inherit Equinox.Codec.Core.IIndexedEvent<byte[]>
            abstract member Stream : string
    /// We assume all Documents represent Events laid out as above
    let parse (d : Document) =
        let x = d.Cast<EventV0>()
        { new IEvent with
              member __.Index = x.i
              member __.IsUnfold = false
              member __.EventType = x.t
              member __.Data = x.d
              member __.Meta = null
              member __.Timestamp = x.c
              member __.Stream = x.s }

let transformV0 catFilter (v0SchemaDocument: Document) : StreamItem seq = seq {
    let parsed = EventV0Parser.parse v0SchemaDocument
    let streamName = (*if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+*)parsed.Stream
    if catFilter (category streamName) then
        yield { stream = streamName; index = parsed.Index; event = parsed } }
//#else
let transformOrFilter catFilter (changeFeedDocument: Document) : StreamItem seq = seq {
    for e in DocumentParser.enumEvents changeFeedDocument do
        if catFilter (category e.Stream) then
            // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
            yield { stream = e.Stream; index = e.Index; event =  e } }
//#endif