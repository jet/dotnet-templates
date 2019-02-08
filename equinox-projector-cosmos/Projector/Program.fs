module ProjectorTemplate.Projector.Program

//#if kafka
open Confluent.Kafka
//#endif
open Equinox.Cosmos
open Equinox.Cosmos.Projection
//#if kafka
open Equinox.Projection.Codec
open Equinox.Projection.Kafka
//#endif
open Equinox.Store
open Microsoft.Azure.Documents.ChangeFeedProcessor
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
//#if kafka
open Newtonsoft.Json
//#endif
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics

module CmdParser =
    open Argu

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    [<NoEquality; NoComparison>]
    type Arguments =
        (* ChangeFeed Args*)
        | [<MainCommand; ExactlyOnce>] LeaseId of string
        | [<AltCommandLine("-s"); Unique>] Suffix of string
        | [<AltCommandLine("-i"); Unique>] ForceStartFromHere
        | [<AltCommandLine("-m"); Unique>] ChangeFeedBatchSize of int
        | [<AltCommandLine("-l"); Unique>] LagFreqS of float
        | [<AltCommandLine("-v"); Unique>] Verbose
        | [<AltCommandLine("-vc"); Unique>] ChangeFeedVerbose
//#if kafka
        (* Kafka Args *)
        | [<AltCommandLine("-b"); Unique>] Broker of string
        | [<AltCommandLine("-t"); Unique>] Topic of string
//#endif
        (* Source Args *)
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<CosmosArguments>
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | LeaseId _ ->          "projector group name."
                | Suffix _ ->           "specify Collection Name suffix (default: `-aux`)."
                | ForceStartFromHere _ -> "(iff `suffix` represents a fresh LeaseId) - force skip to present Position. Default: Never skip an event on a lease."
                | ChangeFeedBatchSize _ -> "maximum item count to supply to Changefeed Api when querying. Default: 1000"
                | LagFreqS _ ->         "specify frequency to dump lag stats. Default: off"
                | Verbose ->            "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->  "request Verbose Logging from ChangeFeedProcessor. Default: off"
//#if kafka
                | Broker _ ->           "specify Kafka Broker, in host:port format. (default: use environment variable EQUINOX_KAFKA_BROKER, if specified)"
                | Topic _ ->            "specify Kafka Topic Id. (default: use environment variable EQUINOX_KAFKA_TOPIC, if specified)"
//#endif
                | Cosmos _ ->           "specify CosmosDb input parameters"
    and Parameters(args : ParseResults<Arguments>) =
        member val Source = CosmosInfo(args.GetResult Cosmos)
//#if kafka
        member val Target = TargetInfo args
//#endif
        member __.LeaseId = args.GetResult LeaseId
        member __.Suffix = args.GetResult(Suffix,"-aux")
        member __.Verbose = args.Contains Verbose
        member __.ChangeFeedVerbose = args.Contains ChangeFeedVerbose
        member __.BatchSize = args.GetResult(ChangeFeedBatchSize,1000)
        member __.LagFrequency = args.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.AuxCollectionName = __.Source.Collection + __.Suffix
        member __.StartFromHere = args.Contains ForceStartFromHere
        member x.BuildChangeFeedParams() =
            Log.Information("Processing {leaseId} in {auxCollName} in batches of {batchSize}", x.LeaseId, x.AuxCollectionName, x.BatchSize)
            if x.StartFromHere then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            { database = x.Source.Database; collection = x.AuxCollectionName}, x.LeaseId, x.StartFromHere, x.BatchSize, x.LagFrequency
//#if kafka
    and TargetInfo(args : ParseResults<Arguments>) =
        member __.Broker = Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "EQUINOX_KAFKA_BROKER")
        member __.Topic = match args.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "EQUINOX_KAFKA_TOPIC"
        member x.BuildTargetParams() = x.Broker, x.Topic
//#endif
    and [<NoEquality; NoComparison>] CosmosArguments =
        | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-rt")>] RetriesWaitTime of int
        | [<AltCommandLine("-s")>] Connection of string
        | [<AltCommandLine("-d")>] Database of string
        | [<AltCommandLine("-c")>] Collection of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                | Retries _ ->          "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                | Database _ ->         "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                | Collection _ ->       "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
    and CosmosInfo(args : ParseResults<CosmosArguments>) =
        member __.Connection =  match args.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =    match args.TryGetResult Database    with Some x -> x | None -> envBackstop "Database" "EQUINOX_COSMOS_DATABASE"
        member __.Collection =  match args.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

        member __.Timeout = args.GetResult(Timeout,5.) |> float |> TimeSpan.FromSeconds
        member __.Mode = args.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Retries = args.GetResult(Retries, 1)
        member __.MaxRetryWaitTime = args.GetResult(RetriesWaitTime, 5)

        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri,masterKey)) = Discovery.FromConnectionString x.Connection
            Log.Information("CosmosDb {mode} {endpointUri} Database {database} Collection {collection}.",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("CosmosDb timeout: {timeout}s, {retries} retries; Throttling maxRetryWaitTime {maxRetryWaitTime}",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c =
                EqxConnector(log=Log.Logger, mode=x.Mode, requestTimeout=x.Timeout,
                    maxRetryAttemptsOnThrottledRequests=x.Retries, maxRetryWaitTimeInSeconds=x.MaxRetryWaitTime)
            (endpointUri,masterKey), c.ConnectionPolicy, { database = x.Database; collection = x.Collection }

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Parameters =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Arguments>(programName = programName)
        parser.ParseCommandLine argv |> Parameters

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose changeLogVerbose =
        Log.Logger <-
            LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
            |> fun c -> let cfpl = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
                            .CreateLogger()

let run (endpointUri, masterKey) connectionPolicy source
        (aux, leaseId, forceSkip, batchSize, lagReportFreq : TimeSpan option)
        createRangeProjector = async {
    let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
        Log.Information("Lags by Range {@rangeLags}", remainingWork)
        return! Async.Sleep interval }
    let maybeLogLag = lagReportFreq |> Option.map logLag
    let! _feedEventHost =
        ChangeFeedProcessor.Start
          ( Log.Logger, endpointUri, masterKey, connectionPolicy, source, aux, leasePrefix = leaseId, forceSkipExistingEvents = forceSkip,
            cfBatchSize = batchSize, createObserver = createRangeProjector, ?reportLagAndAwaitNextEstimation = maybeLogLag)
    do! Async.AwaitKeyboardInterrupt() }

//#if kafka
let mkRangeProjector (broker, topic) =
    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    let cfg = KafkaProducerConfig.Create("ProjectorTemplate", broker, Acks.Leader, compression = LZ4)
    let producer = KafkaProducer.Create(Log.Logger, cfg, topic)
    let disposeProducer = (producer :> IDisposable).Dispose
    let projectBatch (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after CFP hands off to us
        let toKafkaEvent (e: DocumentParser.IEvent) : RenderedEvent = { s = e.Stream; i = e.Index; c = e.EventType; t = e.TimeStamp; d = e.Data; m = e.Meta }
        let pt,events = (fun () -> docs |> Seq.collect DocumentParser.enumEvents |> Seq.map toKafkaEvent |> Array.ofSeq) |> Stopwatch.Time 
        let es = [| for e in events -> e.s, JsonConvert.SerializeObject e |]
        let! et,_ = producer.ProduceBatch es |> Stopwatch.Time
        let r = ctx.FeedResponse
        Log.Information("{range} Fetch: {token} {requestCharge:n0}RU {count} docs {l:n1}s; Parse: {events} events {p:n3}s; Emit: {e:n1}s",
            ctx.PartitionKeyRangeId, r.ResponseContinuation.Trim[|'"'|], r.RequestCharge, docs.Count, float sw.ElapsedMilliseconds / 1000., 
            events.Length, (let e = pt.Elapsed in e.TotalSeconds), (let e = et.Elapsed in e.TotalSeconds))
        sw.Restart() // restart the clock as we handoff back to the CFP
    }
    ChangeFeedObserver.Create(Log.Logger, projectBatch, disposeProducer)
//#else
let createRangeHandler () =
    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    let processBatch (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after CFP hands off to us
        let pt,events = (fun () -> docs |> Seq.collect DocumentParser.enumEvents |> Seq.length) |> Stopwatch.Time
        let r = ctx.FeedResponse
        Log.Information("{range} Fetch: {token} {requestCharge:n0}RU {count} docs {l:n1}s; Parse: {events} events {p:n3}s",
            ctx.PartitionKeyRangeId, r.ResponseContinuation.Trim[|'"'|], r.RequestCharge, docs.Count, float sw.ElapsedMilliseconds / 1000., 
            events, (let e = pt.Elapsed in e.TotalSeconds))
        sw.Restart() // restart the clock as we handoff back to the CFP
    }
    ChangeFeedObserver.Create(Log.Logger, processBatch)
 //#endif
 
[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose args.ChangeFeedVerbose
        let (endpointUri,masterKey), connectionPolicy, source = args.Source.BuildConnectionDetails()
        let aux, leaseId, startFromHere, batchSize, lagFrequency = args.BuildChangeFeedParams()
//#if kafka
        let targetParams = args.Target.BuildTargetParams()
        let createRangeHandler () = mkRangeProjector targetParams
//#endif
        run (endpointUri,masterKey) connectionPolicy source
            (aux, leaseId, startFromHere, batchSize, lagFrequency)
            createRangeHandler     
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1