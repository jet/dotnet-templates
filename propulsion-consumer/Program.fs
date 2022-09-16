module ConsumerTemplate.Program

open Serilog
open System

exception MissingArg of message : string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

type Configuration(tryGet) =

    let get key = match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"
    member _.Broker =                       get "PROPULSION_KAFKA_BROKER"
    member _.Topic =                        get "PROPULSION_KAFKA_TOPIC"
    member _.Group =                        get "PROPULSION_KAFKA_GROUP"

module Args =

    open Argu
    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-b"; Unique>]   Broker of string
        | [<AltCommandLine "-t"; Unique>]   Topic of string
        | [<AltCommandLine "-g"; Unique>]   Group of string
        | [<AltCommandLine "-m"; Unique>]   MaxInflightMb of float
        | [<AltCommandLine "-l"; Unique>]   LagFreqM of float

        | [<AltCommandLine "-w"; Unique>]   MaxDop of int
        | [<AltCommandLine "-V"; Unique>]   Verbose

        interface IArgParserTemplate with
            member p.Usage = p |> function
                | Broker _ ->               "specify Kafka Broker, in host:port format. (optional if environment variable PROPULSION_KAFKA_BROKER specified)."
                | Topic _ ->                "specify Kafka Topic name. (optional if environment variable PROPULSION_KAFKA_TOPIC specified)."
                | Group _ ->                "specify Kafka Consumer Group Id. (optional if environment variable PROPULSION_KAFKA_GROUP specified)."
                | MaxInflightMb _ ->        "maximum MiB of data to read ahead. Default: 10."
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off."

                | MaxDop _ ->               "maximum number of items to process in parallel. Default: 8"
                | Verbose _ ->              "request verbose logging."
    type Arguments(c : Configuration, p : ParseResults<Parameters>) =
        member val Broker =                 p.TryGetResult Broker |> Option.defaultWith (fun () -> c.Broker)
        member val Topic =                  p.TryGetResult Topic  |> Option.defaultWith (fun () -> c.Topic)
        member val Group =                  p.TryGetResult Group  |> Option.defaultWith (fun () -> c.Group)
        member val MaxInFlightBytes =       p.GetResult(MaxInflightMb, 10.) * 1024. * 1024. |> int64
        member val LagFrequency =           p.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes

        member val MaxDop =                 p.GetResult(MaxDop, 8)
        member val Verbose =                p.Contains Verbose

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse tryGetConfigValue argv : Arguments =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        Arguments(Configuration tryGetConfigValue, parser.ParseCommandLine argv)

let [<Literal>] AppName = "ConsumerTemplate"

let start (args : Args.Arguments) =
    let c =
        FsKafka.KafkaConsumerConfig.Create(
            AppName,
            args.Broker, [args.Topic], args.Group, Confluent.Kafka.AutoOffsetReset.Earliest,
            maxInFlightBytes=args.MaxInFlightBytes, ?statisticsInterval=args.LagFrequency)
    //MultiMessages.BatchesSync.Start(c)
    //MultiMessages.BatchesAsync.Start(c, args.MaxDop)
    //NultiMessages.Parallel.Start(c, args.MaxDop)
    MultiStreams.start(c, args.MaxDop)

let run args = async {
    use consumer = start args
    return! consumer.AwaitWithStopOnCancellation()
}

[<EntryPoint>]
let main argv =
    try let args = Args.parse EnvVar.tryGet argv
        try Log.Logger <- LoggerConfiguration().Configure(verbose=args.Verbose).CreateLogger()
            try run args |> Async.RunSynchronously; 0
            with e when not (e :? MissingArg) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with MissingArg msg -> eprintfn "%s" msg; 1
        | :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | e -> eprintf "Exception %s" e.Message; 1
