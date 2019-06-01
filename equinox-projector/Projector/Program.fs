module ProjectorTemplate.Projector.Program

open Equinox.Cosmos
open Equinox.Store // Stopwatch.Time
open Equinox.Cosmos.Projection
open Microsoft.Azure.Documents.ChangeFeedProcessor
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
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
        | [<AltCommandLine("-l"); Unique>] LagFreqM of float
        | [<AltCommandLine("-v"); Unique>] Verbose
        | [<AltCommandLine("-vc"); Unique>] ChangeFeedVerbose
//#if kafka
        (* Kafka Args *)
        | [<AltCommandLine("-b"); Unique>] Broker of string
        | [<AltCommandLine("-t"); Unique>] Topic of string
//#endif
        (* ChangeFeed Args *)
        | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LeaseCollectionSuffix _ -> "specify Collection Name suffix for Leases collection (default: `-aux`)."
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum document count to supply for the Change Feed query. Default: use response size limit"
                | MaxReadAhead _ ->         "maximum number of batches to let processing get ahead of completion. Default: 64"
                | MaxWriters _ ->           "maximum number of concurrent streams on which to process at any time. Default: 1024"
                | LagFreqM _ ->             "specify frequency (minutes) to dump lag stats. Default: off"
                | Verbose ->                "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
//#if kafka
                | Broker _ ->               "specify Kafka Broker, in host:port format. (default: use environment variable EQUINOX_KAFKA_BROKER, if specified)"
                | Topic _ ->                "specify Kafka Topic Id. (default: use environment variable EQUINOX_KAFKA_TOPIC, if specified)"
//#endif
                | Cosmos _ ->               "specify CosmosDb input parameters"
    and Arguments(args : ParseResults<Parameters>) =
        member val Cosmos = Cosmos.Arguments(args.GetResult Cosmos)
//#if kafka
        member val Target = TargetInfo args
//#endif
        member __.LeaseId =                 args.GetResult ConsumerGroupName
        member __.Suffix =                  args.GetResult(LeaseCollectionSuffix,"-aux")
        member __.Verbose =                 args.Contains Verbose
        member __.ChangeFeedVerbose =       args.Contains ChangeFeedVerbose
        member __.MaxDocuments =            args.TryGetResult MaxDocuments
        member __.MaxReadAhead =            args.GetResult(MaxReadAhead,64)
        member __.ConcurrentStreamProcessors = args.GetResult(MaxWriters,1024)
        member __.LagFrequency =            args.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member __.AuxCollectionName =       __.Cosmos.Collection + __.Suffix
        member x.BuildChangeFeedParams() =
            match x.MaxDocuments with
            | None ->
                Log.Information("Processing {leaseId} in {auxCollName} without document count limit (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxCollectionName, x.MaxReadAhead, x.ConcurrentStreamProcessors)
            | Some lim ->
                Log.Information("Processing {leaseId} in {auxCollName} with max {changeFeedMaxDocuments} documents (<= {maxPending} pending) using {dop} processors",
                    x.LeaseId, x.AuxCollectionName, lim, x.MaxReadAhead, x.ConcurrentStreamProcessors)
            if args.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            { database = x.Cosmos.Database; collection = x.AuxCollectionName}, x.LeaseId, args.Contains FromTail, x.MaxDocuments, x.LagFrequency,
            (x.MaxReadAhead, x.ConcurrentStreamProcessors)
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

let createRangeHandler (log : ILogger) (createIngester : ILogger -> Propulsion.Ingestion.Ingester<int*_,_>) mapContent () =
    let mutable rangeIngester = Unchecked.defaultof<_>
    let init rangeLog = async { rangeIngester <- createIngester rangeLog }
    let dispose () = rangeIngester.Stop()
    let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let ingest (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let epoch = ctx.FeedResponse.ResponseContinuation.Trim[|'"'|] |> int64
        // Pass along the function that the coordinator will run to checkpoint past this batch when such progress has been achieved
        let! pt, (cur,max) = rangeIngester.Submit(epoch, ctx.Checkpoint(), (int ctx.PartitionKeyRangeId, mapContent docs)) |> Stopwatch.Time
        let age, readS, postS = DateTime.UtcNow - docs.[docs.Count-1].Timestamp, float sw.ElapsedMilliseconds / 1000., let e = pt.Elapsed in e.TotalSeconds
        log.Information("Read {token,9} age {age:dd\.hh\:mm\:ss} {count,4} docs {requestCharge,6:f1}RU {l,5:f1}s Ingest {pt:f3}s {cur}/{max}",
            epoch, age, docs.Count, ctx.FeedResponse.RequestCharge, readS, postS, cur, max)
        sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
    }
    ChangeFeedObserver.Create(log, ingest, assign=init, dispose=dispose)
 
let run (log : ILogger) discovery connectionPolicy source
        (aux, leaseId, startFromTail, maybeLimitDocumentCount, lagReportFreq : TimeSpan option)
        createObserver = async {
    let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
        log.Information("Backlog {backlog:n0} (by range: {@rangeLags})", remainingWork |> Seq.map snd |> Seq.sum, remainingWork |> Seq.sortBy fst)
        return! Async.Sleep interval }
    let maybeLogLag = lagReportFreq |> Option.map logLag
    let! _feedEventHost =
        ChangeFeedProcessor.Start
          ( log, discovery, connectionPolicy, source, aux, leasePrefix = leaseId, startFromTail = startFromTail,
            createObserver = createObserver, ?reportLagAndAwaitNextEstimation = maybeLogLag, ?maxDocuments = maybeLimitDocumentCount,
            leaseAcquireInterval = TimeSpan.FromSeconds 5., leaseRenewInterval = TimeSpan.FromSeconds 5., leaseTtl = TimeSpan.FromSeconds 10.)
    do! Async.AwaitKeyboardInterrupt() } // exiting will Cancel the child tasks, i.e. the _feedEventHost

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
            |> fun c -> let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
                        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
                        if changeLogVerbose then c else c.Filter.ByExcluding(fun x -> isCfp x)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> c.CreateLogger()

let replaceLongDataWithNull (x : Equinox.Codec.IEvent<_[]>) =
    let data = if x.Data.Length < 900_000 then x.Data else null
    Propulsion.Streams.Internal.EventData.Create(x.EventType,data,x.Meta,x.Timestamp)

let hackDropBigBodies (e : Equinox.Projection.StreamItem) : Propulsion.Streams.StreamItem<_> =
    { stream = e.stream; index = e.index; event = replaceLongDataWithNull e.event }

let mapToStreamItems (docs : Microsoft.Azure.Documents.Document seq) : Propulsion.Streams.StreamItem<_> seq =
    docs
    |> Seq.collect DocumentParser.enumEvents
    // TODO use Seq.filter and/or Seq.map to adjust what's being sent
    |> Seq.map hackDropBigBodies

[<EntryPoint>]
let main argv =
    try try let args = CmdParser.parse argv
            Logging.initialize args.Verbose args.ChangeFeedVerbose
            let discovery, connector, source = args.Cosmos.BuildConnectionDetails()
            let aux, leaseId, startFromTail, maxDocuments, lagFrequency, (maxReadAhead, maxConcurrentStreams) = args.BuildChangeFeedParams()
#if kafka
            let (broker,topic) = args.Target.BuildTargetParams()
#if nostreams
            let nullMapping (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) : seq<Microsoft.Azure.Documents.Document> =
                docs :> _
            let render (doc : Microsoft.Azure.Documents.Document) : string * string =
                let equinoxPartition,documentId = doc.GetPropertyValue "p",doc.Id
                equinoxPartition,Newtonsoft.Json.JsonConvert.SerializeObject {| Id = documentId |}
            let projector =
                Propulsion.Kafka.ParallelProducer.Start(
                    Log.Logger, maxReadAhead, maxConcurrentStreams, "ProjectorTemplate", broker, topic, render, statsInterval=TimeSpan.FromMinutes 1.)
            let createObserver = createRangeHandler Log.Logger projector.StartIngester nullMapping
#else
            let toCodecEvents (span: Propulsion.Streams.StreamSpan<_>) : seq<Equinox.Codec.IEvent<_>> = 
                span.events |> Seq.map (fun e -> Equinox.Codec.Core.EventData.Create(e.EventType, e.Data, e.Meta, e.Timestamp) :> _)
            let render (stream: string, span: Propulsion.Streams.StreamSpan<_>) =
                let events = toCodecEvents span
                let rendered = Equinox.Projection.Codec.RenderedSpan.ofStreamSpan stream span.index events
                Newtonsoft.Json.JsonConvert.SerializeObject(rendered)
            let categorize (streamName : string) = streamName.Split([|'-';'_'|],2).[0]
            let projector =
                Propulsion.Kafka.StreamsProducer.Start(
                    Log.Logger, maxReadAhead, maxConcurrentStreams, "ProjectorTemplate", broker, topic, render,
                    categorize, statsInterval=TimeSpan.FromMinutes 1., stateInterval=TimeSpan.FromMinutes 5.)
            let createObserver = createRangeHandler Log.Logger projector.StartIngester mapToStreamItems
#endif
#else
            let project (_stream, span: Propulsion.Streams.StreamSpan<_>) = async { 
                let r = Random()
                let ms = r.Next(1,span.events.Length)
                do! Async.Sleep ms
                return span.events.Length }
            let categorize (streamName : string) = streamName.Split([|'-';'_'|],2).[0]
            let projector =
                Propulsion.Streams.StreamsProjector.Start(
                    Log.Logger, maxReadAhead, maxConcurrentStreams, project,
                    categorize, statsInterval=TimeSpan.FromMinutes 1., stateInterval=TimeSpan.FromMinutes 5.)
            let createObserver = createRangeHandler Log.Logger projector.StartIngester mapToStreamItems
#endif
            Async.Start(
                run Log.Logger discovery connector.ConnectionPolicy source (aux, leaseId, startFromTail, maxDocuments, lagFrequency) createObserver)
            Async.RunSynchronously(
                projector.AwaitCompletion())
            if projector.RanToCompletion then 0 else 1
        with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
            | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
            | e -> eprintfn "%s" e.Message; 1
    finally Log.CloseAndFlush()