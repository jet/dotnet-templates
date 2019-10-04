/// Follows a feed of messages representing items being added/updated on an aggregate that maintains a list of child items
/// Compared to the Ingester in the `summaryProjector` template, each event is potentially relevant
module ConsumerTemplate.Ingester

open ConsumerTemplate.SkuSummary.Events
open System

/// Defines the shape of input messages on the topic we're consuming
module Contract =

    type OrderInfo = { poNumber : string; reservedUnitQuantity : int }
    type Message =
        {  skuId : SkuId // primary key for the aggregate
           locationId : string
           messageIndex : int64
           pickTicketId : string
           purchaseOrderInfo : OrderInfo[] }
    let parse (utf8 : byte[]) : Message =
        System.Text.Encoding.UTF8.GetString(utf8)
        |> Propulsion.Codec.NewtonsoftJson.Serdes.Deserialize<Message>

type Outcome = Completed of used : int * total : int

/// Gathers stats based on the outcome of each Span processed for emission at intervals controlled by `StreamsConsumer`
type Stats(log, ?statsInterval, ?stateInterval) =
    inherit Propulsion.Kafka.StreamsConsumerStats<Outcome>
        (log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))

    let mutable ok, skipped = 0, 0

    override __.HandleOk res = res |> function
        | Completed (used,total) -> ok <- ok + used; skipped <- skipped + (total-used)

    override __.DumpStats () =
        if ok <> 0 || skipped <> 0 then
            log.Information(" Used {ok} Ignored {skipped}", ok, skipped)
            ok <- 0; skipped <- 0

let (|SkuId|) = SkuId.parse

/// Starts a processing loop accumulating messages by stream - each time we handle all the incoming updates for a give Sku as a single transaction
let startConsumer (config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig) (log : Serilog.ILogger) (service : SkuSummary.Service) maxDop =
    let ingestIncomingSummaryMessage(SkuId skuId, span : Propulsion.Streams.StreamSpan<_>) : Async<Outcome> = async {
        let items =
            [ for e in span.events do
                let x = Contract.parse e.Data
                for o in x.purchaseOrderInfo do
                    yield { locationId = x.locationId
                            messageIndex = x.messageIndex
                            picketTicketId = x.pickTicketId
                            poNumber = o.poNumber
                            reservedQuantity = o.reservedUnitQuantity } ]
        let! used = service.Ingest(skuId, items)
        return Outcome.Completed(used,List.length items)
    }
    let stats = Stats(log)
    // No categorization required, our inputs are all one big family defying categorization
    let category _streamName = "Sku"
    let sequencer = StreamKeyEventSequencer()
    Propulsion.Kafka.StreamsConsumer.Start(log, config, sequencer.ToStreamEvent, ingestIncomingSummaryMessage, maxDop, stats, category)