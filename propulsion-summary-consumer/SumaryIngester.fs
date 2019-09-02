/// Follows a feed of updates, holding the most recently observed one
/// Each update recieved is take to completely supersede all previous updates
module ConsumerTemplate.SummaryIngester

open System
open System.Runtime.Serialization

/// Defines the contract we share with the SummaryProjector's published feed
module TodoUpdates =

    /// A single Item in the Todo List
    type ItemInfo = { id: int; order: int; title: string; completed: bool }

    /// All data summarized for Summary Event Stream
    type SummaryInfo = { items : ItemInfo[] }

    type Message =
        | [<DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Message>()
    let [<Literal>] categoryId = "TodoSummary"

[<RequireQualifiedAccess>]
type Outcome = NoRelevantEvents of count : int | Ok of count : int | Skipped of count : int

type Stats(log, ?statsInterval, ?stateInterval) =
    inherit Propulsion.Kafka.StreamsConsumerStats<Outcome>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))

    let mutable (ok, na, redundant) = 0, 0, 0

    override __.HandleOk res = res |> function
        | Outcome.Ok count -> ok <- ok + 1; redundant <- redundant + count - 1
        | Outcome.Skipped count -> redundant <- redundant + count
        | Outcome.NoRelevantEvents count -> na <- na + count

    override __.DumpStats () =
        if ok <> 0 || na <> 0 || redundant <> 0 then
            log.Information(" Used {ok} Ignored {skipped} N/A {na}", ok, redundant, na)
            ok <- 0; na <- 0 ; redundant <- 0

let startConsumer (config : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig) (log : Serilog.ILogger) (service : TodoSummary.Service) maxDop =
    let (|ClientId|) (value : string) = ClientId.parse value
    let (|DecodeNewest|_|) (codec : FsCodec.IUnionEncoder<_,_>) (stream, span : Propulsion.Streams.StreamSpan<_>) =
        StreamCodec.tryPickBack log codec (stream,span)
    let map : TodoUpdates.Message -> TodoSummary.Events.SummaryData = function
        | TodoUpdates.Summary x ->
            { items =
                [| for x in x.items ->
                    { id = x.id; order = x.order; title = x.title; completed = x.completed } |]}
    let ingestIncomingSummaryMessage (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<Outcome> = async {
        match stream, (stream,span) with
        | Category (TodoUpdates.categoryId, ClientId clientId), DecodeNewest TodoUpdates.codec (version,update) ->
            match! service.Ingest(clientId,version,map update) with
            | true -> return Outcome.Ok span.events.Length
            | false -> return Outcome.Skipped span.events.Length
        | _ -> return Outcome.NoRelevantEvents span.events.Length
    }
    let parseStreamSummaries(KeyValue (_streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
        Propulsion.Codec.NewtonsoftJson.RenderedSummary.parseStreamSummaries(spanJson)
    let stats = Stats(log)
    Propulsion.Kafka.StreamsConsumer.Start(log, config, maxDop, parseStreamSummaries, ingestIncomingSummaryMessage, stats, category)