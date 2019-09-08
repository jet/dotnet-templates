// TODO replace with summary-projector template
module ConsumerTemplate.Publisher

open FSharp.UMX
open Newtonsoft.Json
open Serilog
open System

(* compile time-only strongly typed ids; See https://github.com/fsprojects/FSharp.UMX *)

[<Measure>] type reservationId 
[<Measure>] type skuId

type ReservationId = string<reservationId>
type SkuId = string<skuId>

[<JsonConverter(typeof<FsCodec.NewtonsoftJson.TypeSafeEnumConverter>)>]
type ReservationRejectionReason =
    | LocationNotFound
    | NotEnoughInventory
    | InvalidLocation
    | BadRequest
    | SystemWriteFailure

module Input =

    type InventoryReservationConfirmed =
        {   reservationId : string<reservationId> }
    type InventoryReservationRejected =
        {   reservationId : string<reservationId>
            reason : ReservationRejectionReason }
    type Event = 
        | InventoryReservationConfirmed of InventoryReservationConfirmed
        | InventoryReservationRejected of InventoryReservationRejected
        interface TypeShape.UnionContract.IUnionContract // see https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/

    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let tryDecode = StreamCodec.tryDecode codec
    let [<Literal>] CategoryId = "Inventory"

module Output =

    type ReservationConfirmation =
        {   reservationId : string<reservationId> }
    type ReservationRejection =
        {   reservationId : string<reservationId>
            reason : ReservationRejectionReason }
    type Event =
        | Rejection of ReservationRejection
        | Confirmation of ReservationConfirmation
        interface TypeShape.UnionContract.IUnionContract // see https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

[<AutoOpen>]
module EventEncoder =

    /// Standard form of an Inter-BC message
    type [<NoEquality; NoComparison>] EmittedEvent =
        {   /// Event Type associated with event data in `d`
            c: string

            /// Event body, as UTF-8 encoded json ready to be injected directly into the Json being rendered
            [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.VerbatimUtf8JsonConverter>)>]
            d: byte[] }
    let ofEvent (x:  FsCodec.IEvent<_>) =
        {   c = x.EventType
            d = x.Data }

module Processor =

    type Outcome = Processed of confirmed: int * rejected: int

    type Handler(log, producer : Propulsion.Kafka.Producer) =
        let render (x : Output.Event) : string =
            let encoded = Output.codec.Encode x
            let enveloped = EventEncoder.ofEvent encoded
            JsonConvert.SerializeObject(enveloped)

        let emit (x : Input.Event) = async {
            match x with
            | Input.InventoryReservationConfirmed e ->
                let msg : Output.Event = Output.Confirmation { reservationId = e.reservationId }
                let! _ = producer.ProduceAsync(%e.reservationId, render msg)
                return 1, 0
            | Input.InventoryReservationRejected e ->
                let msg : Output.Event = Output.Rejection { reservationId = e.reservationId; reason = e.reason }
                let! _ = producer.ProduceAsync(%e.reservationId, render msg)
                return 0, 1
        }

        member __.Handle(streamName : string, span : Propulsion.Streams.StreamSpan<_>) = async {
            let mutable confirms, rejects = 0, 0
            for x in span.events |> Seq.choose (Input.tryDecode log streamName) do
                let! c, r = emit x
                confirms <- confirms + c; rejects <- rejects + r
            return Processed (confirms, rejects)
        }

    type Stats(log, ?statsInterval, ?stateInterval) =
        inherit Propulsion.Kafka.StreamsConsumerStats<Outcome>(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg stateInterval (TimeSpan.FromMinutes 5.))

        let mutable confirms, rejects = 0, 0

        override __.HandleOk res = res |> function
            | Processed(confirmed, rejected) ->
                confirms <- confirms + confirmed
                rejects <- rejects + rejected

        override __.DumpStats() =
            if rejects <> 0 || confirms <> 0 then
                log.Information(" Processed Confirms {confirms} Rejects {rejects}", confirms, rejects)
                confirms <- 0; rejects <- 0

    let private enumStreamEvents(KeyValue (streamName : string, spanJson)) : seq<Propulsion.Streams.StreamEvent<_>> =
        match streamName with 
        | Category (Input.CategoryId,_) -> Propulsion.Codec.NewtonsoftJson.RenderedSpan.parse spanJson
        | _ -> Seq.empty

    let log = Log.ForContext<Handler>()

    let startConsumer (consumerConfig : Jet.ConfluentKafka.FSharp.KafkaConsumerConfig, producer, degreeOfParallelism : int) =
        let handler, stats = Handler(log, producer), Stats(log, TimeSpan.FromSeconds 30., TimeSpan.FromMinutes 5.)
        Propulsion.Kafka.StreamsConsumer.Start(
            log, consumerConfig, degreeOfParallelism,
            enumStreamEvents, handler.Handle, stats, StreamNameParser.category,
            logExternalState = producer.DumpStats)
    
    let start maxInflightBytes lagFrequency maxDop broker sourceTopics consumerGroup targetTopic =
        let clientId, mem, statsFreq = "InventoryConfirmRejectProjection", maxInflightBytes, lagFrequency
        let c = Jet.ConfluentKafka.FSharp.KafkaConsumerConfig.Create(clientId, broker, sourceTopics, consumerGroup, maxInFlightBytes = mem, ?statisticsInterval = statsFreq)
        let p = Propulsion.Kafka.Producer(log, clientId, broker, targetTopic)
   
        startConsumer(c, p, maxDop)