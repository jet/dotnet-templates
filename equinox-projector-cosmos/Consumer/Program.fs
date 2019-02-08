module ProjectorTemplate.Consumer.Program

open Equinox.Projection.Kafka
open Serilog
open System
open System.Threading

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
        let codec = Equinox.UnionCodec.JsonUtf8.Create<Event>(settings)

    // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
    module Favorites =
        type Favorited =        { date: DateTimeOffset; skuId: SkuId }
        type Unfavorited =      { skuId: SkuId }

        type Event =
            | Favorited         of Favorited
            | Unfavorited       of Unfavorited
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.UnionCodec.JsonUtf8.Create<Event>(settings)
    
    let tryExtractCategory (x : RenderedEvent) =
        x.s.Split([|'-'|],2,StringSplitOptions.RemoveEmptyEntries)
        |> Array.tryHead
    let tryDecode (log : ILogger) (codec : Equinox.UnionCodec.IUnionEncoder<_,_>) (x : RenderedEvent) =
        match codec.TryDecode { caseName = x.c; payload = x.d } with
        | None ->
            if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                log.ForContext("event", System.Text.Encoding.UTF8.GetString(x.d), true)
                    .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, x.c, x.s);
            None
        | Some e -> Some e

    // Example of filtering our relevant Events from the Kafka stream
    // NB if the percentage of relevant events is low, one may wish to adjust the projector to project only a subset
    type Interpreter() =
        let log = Log.ForContext<Interpreter>()

        /// Handles various category / eventType / payload types as produced by Equinox.Tool
        member __.TryDecode(x : Confluent.Kafka.ConsumeResult<_,_>) =
            let ke = JsonConvert.DeserializeObject<RenderedEvent>(x.Value)
            match tryExtractCategory ke with
            | Some "Favorites" -> tryDecode log Favorites.codec ke |> Option.map Choice1Of2
            | Some "SavedForLater" -> tryDecode log SavedForLater.codec ke |> Option.map Choice2Of2
            | x ->
                if log.IsEnabled Serilog.Events.LogEventLevel.Debug then
                    log.ForContext("event", ke).Debug("Event could not be interpreted due to unknown category {category}", x)
                None

module Consumer =
    open EventParser

    /// Starts a consumer which will will be driven based on batches emanating from the supplied `cfg`
    let start (cfg: KafkaConsumerConfig) (degreeOfParallelism: int) =
        let log = Log.ForContext<KafkaConsumer>()
        let dop = new SemaphoreSlim(degreeOfParallelism)
        let decoder = Interpreter()
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
            let! _ =
                msgs
                |> Seq.choose decoder.TryDecode
                |> Seq.map handle
                |> Seq.map dop.Throttle
                |> Async.Parallel
            log.Information("Consumed {b} Favorited {f} Unfavorited {u} Saved {s} Cleared {c}",
                Array.length msgs, favorited, unfavorited, saved, cleared)
        }
        KafkaConsumer.Start log cfg consume

module CmdParser =
    open Argu

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    [<NoEquality; NoComparison>]
    type Arguments =
        | [<AltCommandLine("-b"); Unique>] Broker of string
        | [<AltCommandLine("-t"); Unique>] Topic of string
        | [<AltCommandLine("-g"); Unique>] Group of string
        | [<AltCommandLine("-i"); Unique>] Parallelism of int
        | [<AltCommandLine("-v"); Unique>] Verbose

        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Broker _ ->   "specify Kafka Broker, in host:port format. (optional if environment variable EQUINOX_KAFKA_BROKER specified)"
                | Topic _ ->    "specify Kafka Topic name. (optional if environment variable EQUINOX_KAFKA_TOPIC specified)"
                | Group _ ->    "specify Kafka Consumer Group Id. (optional if environment variable EQUINOX_KAFKA_GROUP specified)"
                | Parallelism _ -> "parallelism constraint when handling batches being consumed (default: 2 * Environment.ProcessorCount)"
                | Verbose _ ->  "request verbose logging."

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : ParseResults<Arguments> =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Arguments>(programName = programName)
        parser.ParseCommandLine argv

    type Parameters(args : ParseResults<Arguments>) =
        member __.Broker = Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "EQUINOX_KAFKA_BROKER")
        member __.Topic = match args.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "EQUINOX_KAFKA_TOPIC"
        member __.Group = match args.TryGetResult Group with Some x -> x | None -> envBackstop "Group" "EQUINOX_KAFKA_GROUP"
        member __.Parallelism = match args.TryGetResult Parallelism with Some x -> x | None -> 2 * Environment.ProcessorCount
        member __.Verbose = args.Contains Verbose

module Logging =
    let initialize verbose =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                        if not verbose then c.WriteTo.Console(theme=theme)
                        else c.WriteTo.Console(theme=theme, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}|{Properties}{NewLine}{Exception}")
            |> fun c -> c.CreateLogger()

[<EntryPoint>]
let main argv =
    try let parsed = CmdParser.parse argv
        let args = CmdParser.Parameters(parsed)
        Logging.initialize args.Verbose
        let cfg = KafkaConsumerConfig.Create("ProjectorTemplate", args.Broker, [args.Topic], args.Group)

        use c = Consumer.start cfg args.Parallelism
        c.AwaitCompletion() |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> Log.Fatal(e, "Exiting"); 1