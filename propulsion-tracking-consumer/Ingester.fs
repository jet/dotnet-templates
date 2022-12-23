/// Follows a feed of messages representing items being added/updated on an aggregate that maintains a list of child items
/// Compared to the Ingester in the `proReactor` template, each event is potentially relevant
module ConsumerTemplate.Ingester

open Propulsion.Internal

/// Defines the shape of input messages on the topic we're consuming
module Contract =

    type OrderInfo = { poNumber : string; reservedUnitQuantity : int }
    type Message =
        {  skuId : SkuId // primary key for the aggregate
           locationId : string
           messageIndex : int64
           pickTicketId : string
           purchaseOrderInfo : OrderInfo[] }
    let serdes = FsCodec.SystemTextJson.Options.Default |> FsCodec.SystemTextJson.Serdes
    let parse (utf8 : Propulsion.Streams.Default.EventBody) : Message =
        // NB see https://github.com/jet/FsCodec for details of the default serialization profile (TL;DR only has an `OptionConverter`)
        System.Text.Encoding.UTF8.GetString(utf8.Span)
        |> serdes.Deserialize<Message>

type Outcome = Completed of used : int * unused : int

/// Gathers stats based on the outcome of each Span processed for emission at intervals controlled by `StreamsConsumer`
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, skipped = 0, 0

    override _.HandleOk res = res |> function
        | Completed (used, unused) -> ok <- ok + used; skipped <- skipped + unused
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 then
            log.Information(" Used {ok} Skipped {skipped}", ok, skipped)
            ok <- 0; skipped <- 0

/// Ingest queued events per sku - each time we handle all the incoming updates for a given stream as a single act
let ingest
        (service : SkuSummary.Service)
        (FsCodec.StreamName.CategoryAndId (_, SkuId.Parse skuId)) (span : Propulsion.Streams.StreamSpan<_>) ct = Async.startImmediateAsTask ct <| async {
    let items =
        [ for e in span do
            let x = Contract.parse e.Data
            for o in x.purchaseOrderInfo do
                let x : SkuSummary.Events.ItemData =
                    {   locationId = x.locationId
                        messageIndex = x.messageIndex
                        picketTicketId = x.pickTicketId
                        poNumber = o.poNumber
                        reservedQuantity = o.reservedUnitQuantity }
                yield x ]
    let! used = service.Ingest(skuId, items)
    return struct (Propulsion.Streams.SpanResult.AllProcessed, Outcome.Completed(used, items.Length - used)) }
