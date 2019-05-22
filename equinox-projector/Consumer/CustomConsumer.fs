module ProjectorTemplate.Consumer.CustomConsumer

open Serilog
open System

[<AutoOpen>]
module EventParser =
    open Equinox.Projection.Codec
    open Newtonsoft.Json

    type SkuId = string

    let settings = JsonSerializerSettings()

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module SavedForLater =
        type Item =             { skuId : SkuId; dateSaved : DateTimeOffset }

        type Added =            { skus : SkuId []; dateSaved : DateTimeOffset }
        type Removed =          { skus : SkuId [] }
        type Merged =           { items : Item [] }

        type Event =
            /// Inclusion of another set of state in this one
            | Merged of Merged
            /// Removal of a set of skus
            | Removed of Removed
            /// Addition of a collection of skus to the list
            | Added of Added
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(settings)

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module Favorites =
        type Favorited =        { date: DateTimeOffset; skuId: SkuId }
        type Unfavorited =      { skuId: SkuId }

        type Event =
            | Favorited         of Favorited
            | Unfavorited       of Unfavorited
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(settings)
    
    let tryExtractCategory (stream : string) =
        stream.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries)
        |> Array.tryHead
    let tryDecode (log : ILogger) (stream : string) (codec : Equinox.Codec.IUnionEncoder<_,_>) (x : Equinox.Codec.IEvent<byte[]>) =
        match codec.TryDecode x with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.Data), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.EventType, stream);
            None
        | Some e -> Some e

    // Example of filtering our relevant Events from the Kafka stream
    // NB if the percentage of relevant events is low, one may wish to adjust the projector to project only a subset
    type Interpreter() =
        let log = Log.ForContext<Interpreter>()

        /// Handles various category / eventType / payload types as produced by Equinox.Tool
        member __.TryDecode(x : Confluent.Kafka.ConsumeResult<_,_>) = seq {
            let span = JsonConvert.DeserializeObject<RenderedSpan>(x.Value)
            let events = span |> RenderedSpan.enumEvents
            match tryExtractCategory span.s with
            | Some "Favorites" -> yield! events |> Seq.choose (tryDecode log span.s Favorites.codec >> Option.map Choice1Of2)
            | Some "SavedForLater" -> yield! events |> Seq.choose (tryDecode log span.s SavedForLater.codec >> Option.map Choice2Of2)
            | x ->
                if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                    log.ForContext("span", span).Debug("Span could not be interpreted due to unknown category {category}", x)
                () }

open Jet.ConfluentKafka.FSharp
open System.Threading

/// Starts a consumer which will will be driven based on batches emanating from the supplied `cfg`
let start (cfg: KafkaConsumerConfig) (degreeOfParallelism: int) =
    let log = Log.ForContext<BatchedConsumer>()
    let dop = new SemaphoreSlim(degreeOfParallelism)
    let decoder = EventParser.Interpreter()
    let consume msgs = async {
        // TODO filter relevant events, fan our processing as appropriate
        let mutable favorited, unfavorited, saved, cleared = 0, 0, 0, 0 
        let handleFave = function
            | Favorites.Favorited _ -> Interlocked.Increment &favorited |> ignore
            | Favorites.Unfavorited _ -> Interlocked.Increment &unfavorited |> ignore
        let handleSave = function
            | SavedForLater.Added e -> Interlocked.Add(&saved,e.skus.Length) |> ignore
            | SavedForLater.Removed e -> Interlocked.Add(&cleared,e.skus.Length) |> ignore
            | SavedForLater.Merged e -> Interlocked.Add(&saved,e.items.Length) |> ignore
        // While this does not need to be async in this toy case, we illustrate here as any real example will need to deal with it
        let handle e = async {
            match e with
            | Choice1Of2 fe -> handleFave fe
            | Choice2Of2 se -> handleSave se
        }
        let! res =
            msgs
            |> Seq.collect decoder.TryDecode
            |> Seq.map (handle >> dop.Throttle)
            |> Async.Parallel
        log.Information("Consumed {b} Favorited {f} Unfavorited {u} Saved {s} Cleared {c}",
            Array.length res, favorited, unfavorited, saved, cleared)
    }
    BatchedConsumer.Start(log, cfg, consume)