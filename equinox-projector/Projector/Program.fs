﻿module ProjectorTemplate.Projector.Program

//#if kafka
open Confluent.Kafka
//#endif
open Equinox.Cosmos
open Equinox.Cosmos.Projection
//#if kafka
open Equinox.Projection.Codec
open Equinox.Store
open Jet.ConfluentKafka.FSharp
//#else
open Equinox.Projection
//#endif
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

    module Cosmos =
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
            | [<AltCommandLine("-o")>] Timeout of float
            | [<AltCommandLine("-r")>] Retries of int
            | [<AltCommandLine("-rt")>] RetriesWaitTime of int
            | [<AltCommandLine("-s")>] Connection of string
            | [<AltCommandLine("-d")>] Database of string
            | [<AltCommandLine("-c")>] Collection of string
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                    | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                    | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                    | Database _ ->         "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                    | Collection _ ->       "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode = a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
            member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

            member __.Timeout = a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Retries = a.GetResult(Retries, 1)
            member __.MaxRetryWaitTime = a.GetResult(RetriesWaitTime, 5)

            member x.BuildConnectionDetails() =
                let (Discovery.UriAndKey (endpointUri,_) as discovery) = Discovery.FromConnectionString x.Connection
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Collection {collection}.",
                    x.Mode, endpointUri, x.Database, x.Collection)
                Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let connector = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
                discovery, connector, { database = x.Database; collection = x.Collection } 

    [<NoEquality; NoComparison>]
    type Parameters =
        (* ChangeFeed Args*)
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine("-s"); Unique>] LeaseCollectionSuffix of string
        | [<AltCommandLine("-z"); Unique>] FromTail
        | [<AltCommandLine("-md"); Unique>] MaxDocuments of int
        | [<AltCommandLine "-r"; Unique>] MaxReadAhead of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
        | [<AltCommandLine("-l"); Unique>] LagFreqS of float
        | [<AltCommandLine("-v"); Unique>] Verbose
        | [<AltCommandLine("-vc"); Unique>] ChangeFeedVerbose
//#if kafka
        (* Kafka Args *)
        | [<AltCommandLine("-b"); Unique>] Broker of string
        | [<AltCommandLine("-t"); Unique>] Topic of string
//#else
        | [<AltCommandLine "-rw"; Unique>] MaxSubmissionsPerRange of int
//#endif
        (* ChangeFeed Args *)
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LeaseCollectionSuffix _ -> "specify Collection Name suffix for Leases collection (default: `-aux`)."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maxiumum document count to supply for the Change Feed query. Default: use response size limit"
                | MaxReadAhead _ ->         "Maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "Maximum number of concurrent streams on which to process at any time. Default: 1024"
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | Verbose ->                "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
//#if kafka
                | Broker _ ->               "specify Kafka Broker, in host:port format. (default: use environment variable EQUINOX_KAFKA_BROKER, if specified)"
                | Topic _ ->                "specify Kafka Topic Id. (default: use environment variable EQUINOX_KAFKA_TOPIC, if specified)"
//#else
                | MaxSubmissionsPerRange _ -> "Maximum number of batches to submit to the processor at any time. Default: 32"
//#endif
                | Cosmos _ ->               "specify CosmosDb input parameters"
    and Arguments(args : ParseResults<Parameters>) =
        member val Cosmos = Cosmos.Arguments(args.GetResult Cosmos)
//#if kafka
        member val Target = TargetInfo args
//#else
        member __.MaxSubmissionPerRange =   args.GetResult(MaxSubmissionsPerRange,32)
//#endif
        member __.LeaseId =                 args.GetResult ConsumerGroupName
        member __.Suffix =                  args.GetResult(LeaseCollectionSuffix,"-aux")
        member __.Verbose =                 args.Contains Verbose
        member __.ChangeFeedVerbose =       args.Contains ChangeFeedVerbose
        member __.MaxDocuments =            args.TryGetResult MaxDocuments
        member __.MaxReadAhead =            args.GetResult(MaxReadAhead,64)
        member __.ProcessorDop =            args.GetResult(MaxWriters,64)
        member __.LagFrequency =            args.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.AuxCollectionName =       __.Cosmos.Collection + __.Suffix
        member x.BuildChangeFeedParams() =
            match x.MaxDocuments with
            | None ->
                Log.Information("Processing {leaseId} in {auxCollName} without document count limit (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxCollectionName, x.MaxReadAhead, x.ProcessorDop)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxCollName} with max {changeFeedMaxDocuments} documents (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxCollectionName, lim, x.MaxReadAhead, x.ProcessorDop)
            if args.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            { database = x.Cosmos.Database; collection = x.AuxCollectionName}, x.LeaseId, args.Contains FromTail, x.MaxDocuments, (x.MaxReadAhead, x.MaxSubmissionPerRange, x.ProcessorDop), x.LagFrequency
//#if kafka
    and TargetInfo(args : ParseResults<Parameters>) =
        member __.Broker = Uri(match args.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "EQUINOX_KAFKA_BROKER")
        member __.Topic = match args.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "EQUINOX_KAFKA_TOPIC"
        member x.BuildTargetParams() = x.Broker, x.Topic
//#endif

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

let run (log : ILogger) discovery connectionPolicy source
        (aux, leaseId, startFromTail, maybeLimitDocumentCount, lagReportFreq : TimeSpan option)
        createRangeProjector = async {
    let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
        log.Information("Backlog {backlog:n0} (by range: {@rangeLags})", remainingWork |> Seq.map snd |> Seq.sum, remainingWork |> Seq.sortByDescending snd)
        return! Async.Sleep interval }
    let maybeLogLag = lagReportFreq |> Option.map logLag
    let! _feedEventHost =
        ChangeFeedProcessor.Start
          ( log, discovery, connectionPolicy, source, aux, leasePrefix = leaseId, startFromTail = startFromTail,
            createObserver = createRangeProjector, ?maxDocuments = maybeLimitDocumentCount, ?reportLagAndAwaitNextEstimation = maybeLogLag)
    do! Async.AwaitKeyboardInterrupt() }

//#if kafka
let mkRangeProjector (broker, topic) log (_maxPendingBatches,_maxDop,_project) _categorize () =
    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    let cfg = KafkaProducerConfig.Create("ProjectorTemplate", broker, Acks.Leader, compression = CompressionType.Lz4)
    let producer = KafkaProducer.Create(Log.Logger, cfg, topic)
    let disposeProducer = (producer :> IDisposable).Dispose
    let ingest (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let pt,events = (fun () -> docs |> Seq.collect DocumentParser.enumEvents |> Seq.map RenderedEvent.ofStreamItem |> Array.ofSeq) |> Stopwatch.Time 
        let es = [| for e in events -> e.s, JsonConvert.SerializeObject e |]
        let! et,() = async {
            // TODO handle failure
            let! _ = producer.ProduceBatch es
            return! ctx.Checkpoint() |> Stopwatch.Time }
        log.Information("Read {token,8} {count,4} docs {requestCharge,4}RU {l:n1}s Parse {events,5} events {p:n3}s Emit {e:n1}s",
            ctx.FeedResponse.ResponseContinuation.Trim[|'"'|], docs.Count, int ctx.FeedResponse.RequestCharge,
            float sw.ElapsedMilliseconds / 1000., events.Length, (let e = pt.Elapsed in e.TotalSeconds), (let e = et.Elapsed in e.TotalSeconds))
        sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
    }
    ChangeFeedObserver.Create(log, ingest, dispose=disposeProducer)
//#else
let createRangeHandler (log:ILogger) (maxReadAhead, maxSubmissionsPerRange, projectionDop) categorize project =
    let scheduler = Projector.Start (log, projectionDop, project, categorize, TimeSpan.FromMinutes 1.)
    fun () ->
        let mutable rangeIngester = Unchecked.defaultof<IIngester>
        let init rangeLog = async { rangeIngester <- Ingester.Start (rangeLog, scheduler, maxReadAhead, maxSubmissionsPerRange, categorize) }
        let ingest epoch checkpoint docs =
            let items = Seq.collect DocumentParser.enumEvents docs
            rangeIngester.Submit(epoch, checkpoint, items)
        let dispose () = rangeIngester.Stop()
        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let ingest (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let epoch = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64
            // Pass along the function that the coordinator will run to checkpoint past this batch when such progress has been achieved
            let checkpoint = async { do! ctx.CheckpointAsync() |> Async.AwaitTaskCorrect }
            let! pt, (cur,max) = ingest epoch checkpoint docs |> Stopwatch.Time
            log.Information("Read {token,8} {count,4} docs {requestCharge,4}RU {l:n1}s Post {pt:n3}s {cur}/{max}",
                epoch, docs.Count, int ctx.FeedResponse.RequestCharge, float sw.ElapsedMilliseconds / 1000.,
                let e = pt.Elapsed in e.TotalSeconds, cur, max)
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, ingest, assign=init, dispose=dispose)
 //#endif
 
// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose changeLogVerbose =
        Log.Logger <-
            LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            // LibLog writes to the global logger, so we need to control the emission
            |> fun c -> let cfpl = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()

[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose args.ChangeFeedVerbose
        let discovery, connector, source = args.Cosmos.BuildConnectionDetails()
        let aux, leaseId, startFromTail, maxDocuments, limits, lagFrequency = args.BuildChangeFeedParams()
//#if kafka
        //let targetParams = args.Target.BuildTargetParams()
        //let createRangeHandler = mkRangeProjector targetParams
//#endif
        let project (batch : Equinox.Projection.Buffer.StreamSpan) = async { 
            let r = Random()
            let ms = r.Next(1,batch.span.events.Length * 10)
            do! Async.Sleep ms
            return batch.span.events.Length }
        let categorize (streamName : string) = streamName.Split([|'-';'_'|],2).[0]
        run Log.Logger discovery connector.ConnectionPolicy source
            (aux, leaseId, startFromTail, maxDocuments, lagFrequency)
            (createRangeHandler Log.Logger limits categorize project)
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1