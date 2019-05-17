﻿module ProjectorTemplate.Consumer.Program

open Serilog
open System

module CmdParser =
    open Argu

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    [<NoEquality; NoComparison>]
    type Parameters =
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
    let parse argv : ParseResults<Parameters> =
        let programName = Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv

    type Arguments(args : ParseResults<Parameters>) =
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
        let args = CmdParser.Arguments(parsed)
        Logging.initialize args.Verbose
        let cfg = Jet.ConfluentKafka.FSharp.KafkaConsumerConfig.Create("ProjectorTemplate", args.Broker, [args.Topic], args.Group)

        use c = CustomConsumer.start cfg args.Parallelism
        c.AwaitCompletion() |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> Log.Fatal(e, "Exiting"); 1