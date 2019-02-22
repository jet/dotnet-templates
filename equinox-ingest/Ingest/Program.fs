module ProjectorTemplate.Ingester.Program

open Equinox.Cosmos
open Equinox.Store
open FSharp.Control
open Serilog
open System
open Equinox.EventStore

module CmdParser =
    open Argu
    type LogEventLevel = Serilog.Events.LogEventLevel

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    module Cosmos =
        type [<NoEquality; NoComparison>] Arguments =
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
        type Info(args : ParseResults<Arguments>) =
            member __.Connection =  match args.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =    match args.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Collection =  match args.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

            member __.Timeout = args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
            member __.Mode = args.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
            member __.Retries = args.GetResult(Retries, 1)
            member __.MaxRetryWaitTime = args.GetResult(RetriesWaitTime, 5)
            
            /// Connect with the provided parameters and/or environment variables
            member x.Connnect
                /// Connection/Client identifier for logging purposes
                name : Async<CosmosConnection> =
                let (Discovery.UriAndKey (endpointUri,_masterKey)) as discovery = Discovery.FromConnectionString x.Connection
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Collection {collection}.",
                    x.Mode, endpointUri, x.Database, x.Collection)
                Log.Information("CosmosDb timeout: {timeout}s, {retries} retries; Throttling maxRetryWaitTime {maxRetryWaitTime}",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let c =
                    CosmosConnector(log=Log.Logger, mode=x.Mode, requestTimeout=x.Timeout,
                        maxRetryAttemptsOnThrottledRequests=x.Retries, maxRetryWaitTimeInSeconds=x.MaxRetryWaitTime)
                c.Connect(name, discovery)

    /// To establish a local node to run against:
    ///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    ///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
    module EventStore =
        open Equinox.EventStore
        type [<NoEquality; NoComparison>] Arguments =
            | [<AltCommandLine("-v")>] VerboseStore
            | [<AltCommandLine("-o")>] Timeout of float
            | [<AltCommandLine("-r")>] Retries of int
            | [<AltCommandLine("-g")>] Host of string
            | [<AltCommandLine("-u")>] Username of string
            | [<AltCommandLine("-p")>] Password of string
            | [<AltCommandLine("-c")>] ConcurrentOperationsLimit of int
            | [<AltCommandLine("-h")>] HeartbeatTimeout of float
            | [<AltCommandLine("-m"); Unique>] MaxItems of int
            | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Arguments>
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | VerboseStore ->       "Include low level Store logging."
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | Host _ ->             "specify a DNS query, using Gossip-driven discovery against all A records returned (defaults: envvar:EQUINOX_ES_HOST, localhost)."
                    | Username _ ->         "specify a username (defaults: envvar:EQUINOX_ES_USERNAME, admin)."
                    | Password _ ->         "specify a Password (defaults: envvar:EQUINOX_ES_PASSWORD, changeit)."
                    | ConcurrentOperationsLimit _ -> "max concurrent operations in flight (default: 5000)."
                    | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."
                    | MaxItems _ ->         "maximum item count to request. Default: 4096"
                    | Cosmos _ ->           "specify CosmosDb parameters"
        type Info(args : ParseResults<Arguments> ) =
            let connect (log: ILogger) (heartbeatTimeout, col) (operationTimeout, operationRetries) discovery (username, password) connection =
                let log = if log.IsEnabled LogEventLevel.Debug then Logger.SerilogVerbose log else Logger.SerilogNormal log
                GesConnector(username, password, operationTimeout, operationRetries,heartbeatTimeout=heartbeatTimeout,
                        concurrentOperationsLimit=col, log=log, tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
                    .Establish("ProjectorTemplate", discovery, connection)
            member val Cosmos = Cosmos.Info(args.GetResult Cosmos)
            member __.CreateGateway conn = GesGateway(conn, GesBatchingPolicy(maxBatchSize = args.GetResult(MaxItems,4096)))
            member __.Host =     match args.TryGetResult Host       with Some x -> x | None -> envBackstop "Host"       "EQUINOX_ES_HOST"
            member __.User =     match args.TryGetResult Username   with Some x -> x | None -> envBackstop "Username"   "EQUINOX_ES_USERNAME"
            member __.Password = match args.TryGetResult Password   with Some x -> x | None -> envBackstop "Password"   "EQUINOX_ES_PASSWORD"
            member val CacheStrategy = let c = Caching.Cache("ProjectorTemplate", sizeMb = 50) in CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            member __.Connect(log: ILogger, storeLog, connection) =
                let (timeout, retries) as operationThrottling = args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds, args.GetResult(Retries,1)
                let heartbeatTimeout = args.GetResult(HeartbeatTimeout,1.5) |> TimeSpan.FromSeconds
                let concurrentOperationsLimit = args.GetResult(ConcurrentOperationsLimit,5000)
                log.Information("EventStore {host} heartbeat: {heartbeat}s MaxConcurrentRequests {concurrency} Timeout: {timeout}s Retries {retries}",
                    __.Host, heartbeatTimeout.TotalSeconds, concurrentOperationsLimit, timeout.TotalSeconds, retries)
                connect storeLog (heartbeatTimeout, concurrentOperationsLimit) operationThrottling (Discovery.GossipDns __.Host) (__.User,__.Password) connection

    [<NoEquality; NoComparison>]
    type Arguments =
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine "-i"; Unique>] ForceStartFromHere
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-s">] Stream of string
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Es of ParseResults<EventStore.Arguments>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->"projector group name."
                | ForceStartFromHere _ -> "(iff `suffix` represents a fresh LeaseId) - force skip to present Position. Default: Never skip an event on a lease."
                | BatchSize _ ->        "maximum item count to request from feed. Default: 1000"
                | LagFreqS _ ->         "specify frequency to dump lag stats. Default: off"
                | Verbose ->            "request Verbose Logging. Default: off"
                | Stream _ ->           "specify stream(s) to seed the processing with"
                | Es _ ->               "specify EventStore parameters"
    and Parameters(args : ParseResults<Arguments>) =
        member val EventStore = EventStore.Info(args.GetResult Es)
        member __.ConsumerGroupName = args.GetResult ConsumerGroupName
        member __.Verbose = args.Contains Verbose
        member __.BatchSize = args.GetResult(BatchSize,1000)
        member __.LagFrequency = args.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.StartFromHere = args.Contains ForceStartFromHere
        member x.BuildFeedParams() =
            Log.Information("Processing {leaseId} in batches of {batchSize}", x.ConsumerGroupName, x.BatchSize)
            if x.StartFromHere then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            x.ConsumerGroupName, x.StartFromHere, x.BatchSize, x.LagFrequency, args.GetResults Stream

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Parameters =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Arguments>(programName = programName)
        parser.ParseCommandLine argv |> Parameters

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose =
        Log.Logger <-
            LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
                            .CreateLogger()

//let mkRangeProjector (broker, topic) =
//    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
//    let cfg = KafkaProducerConfig.Create("ProjectorTemplate", broker, Acks.Leader, compression = LZ4)
//    let producer = KafkaProducer.Create(Log.Logger, cfg, topic)
//    let disposeProducer = (producer :> IDisposable).Dispose
//    let projectBatch (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
//        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
//        let toKafkaEvent (e: DocumentParser.IEvent) : RenderedEvent = { s = e.Stream; i = e.Index; c = e.EventType; t = e.TimeStamp; d = e.Data; m = e.Meta }
//        let pt,events = (fun () -> docs |> Seq.collect DocumentParser.enumEvents |> Seq.map toKafkaEvent |> Array.ofSeq) |> Stopwatch.Time 
//        let es = [| for e in events -> e.s, JsonConvert.SerializeObject e |]
//        let! et,_ = producer.ProduceBatch es |> Stopwatch.Time
//        let r = ctx.FeedResponse
//        Log.Information("{range} Fetch: {token} {requestCharge:n0}RU {count} docs {l:n1}s; Parse: {events} events {p:n3}s; Emit: {e:n1}s",
//            ctx.PartitionKeyRangeId, r.ResponseContinuation.Trim[|'"'|], r.RequestCharge, docs.Count, float sw.ElapsedMilliseconds / 1000., 
//            events.Length, (let e = pt.Elapsed in e.TotalSeconds), (let e = et.Elapsed in e.TotalSeconds))
//        sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
//    }
//    ChangeFeedObserver.Create(Log.Logger, projectBatch, disposeProducer)

open System.Collections.Concurrent
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

module Ingester =
    open Equinox.Cosmos.Core
    open Equinox.Cosmos.Store

    type [<NoComparison>] Batch = { stream: string; pos: int64; events: IEvent[] }
    type [<NoComparison>] Result =
        | Ok of stream: string * updatedPos: int64
        | Duplicate of stream: string * updatedPos: int64
        | Conflict of overage: Batch
        | Exn of exn: exn * batch: Batch
    let private write (ctx : CosmosContext) (batch : Batch) = async {
        let stream = ctx.CreateStream batch.stream
        try let! res = ctx.Sync(stream, { index = batch.pos; etag = None }, batch.events)
            match res with
            | AppendResult.Ok pos -> return Ok (batch.stream, pos.index) 
            | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                if pos.index >= batch.pos + batch.events.LongLength then return Duplicate (batch.stream, pos.index)
                else return Conflict { stream = batch.stream; pos = pos.index; events = batch.events |> Array.skip (pos.index-(batch.pos+batch.events.LongLength) |> int) }
        with e -> return Exn (e, batch) }

    /// Manages distribution of work across a specified number of concurrent writers
    type Writer (ctx : CosmosContext, queueLen, ct : CancellationToken) =
        let buffer = new BlockingCollection<_>(ConcurrentQueue(), queueLen)
        let result = Event<_>()
        let child = async {
            let! ct = Async.CancellationToken // i.e. cts.Token
            for item in buffer.GetConsumingEnumerable(ct) do
                let! res = write ctx item
                result.Trigger res }
        member internal __.StartConsumers n =
            for _ in 1..n do
                Async.StartAsTask(child, cancellationToken=ct) |> ignore

        /// Supply an item to be processed
        member __.TryAdd(item, timeout : TimeSpan) = buffer.TryAdd(item, int timeout.TotalMilliseconds, ct)
        [<CLIEvent>] member __.Result = result.Publish
        // TEMP - to be removed
        member __.Add item = buffer.Add item

    type Queue(log: Serilog.ILogger, writer : Writer) =
        member __.TryAdd(item, timeout) = writer.TryAdd(item, timeout)
        // TEMP - this blocks, and real impl wont do that
        member __.Post item =
            writer.Add item
        member __.HandleWriteResult res =
            match res with
            | Ok (stream, pos) -> log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | Duplicate (stream, pos) -> log.Information("Ignored   {stream} (synced up to {pos})", stream, pos)
            | Conflict overage -> log.Information("Requeing  {stream} {pos} ({count} events)", overage.stream, overage.pos, overage.events.Length)
            | Exn (exn, batch) ->
                log.Warning(exn,"Writing   {stream} failed, retrying {count} events ....", batch.stream, batch.events.Length)
                // TODO remove; this is not a sustainable idea
                writer.Add batch
    type Ingester(writer : Writer) =
        member __.Add batch = writer.Add batch

    /// Manages establishing of the writer 'threads' - can be Stop()ped explicitly and/or will stop when caller does
    let start(ctx : Equinox.Cosmos.Core.CosmosContext, writerQueueLen, writerCount) = async {
        let! ct = Async.CancellationToken
        let writer = Writer(ctx, writerQueueLen, ct)
        let queue = Queue(Log.Logger,writer)
        let _ = writer.Result.Subscribe queue.HandleWriteResult // codependent, wont worry about unsubcribing
        writer.StartConsumers writerCount
        return Ingester(writer)
    }

module Reader =
    open EventStore.ClientAPI

    let loadSpecificStreamsTemp (conn: IEventStoreConnection, batchSize) streams (postBatch : (Ingester.Batch -> unit)) =
        let fetchStream stream =
            let rec fetchFrom pos = async {
                let! currentSlice = conn.ReadStreamEventsBackwardAsync(stream, pos, batchSize, resolveLinkTos=true) |> Async.AwaitTaskCorrect
                if currentSlice.IsEndOfStream then return () else
                let events =
                    [| for x in currentSlice.Events ->
                        let e = x.Event
                        Equinox.Cosmos.Store.EventData.Create (e.EventType, e.Data, e.Metadata) :> Equinox.Cosmos.Store.IEvent |]
                postBatch { stream = stream; pos = currentSlice.FromEventNumber; events = events }
                return! fetchFrom currentSlice.NextEventNumber }
            fetchFrom 0L
        async {
            for stream in streams do
                do! fetchStream stream }

open EventStore.ClientAPI
let run (destination : CosmosConnection, colls) (source : GesConnection) (leaseId, forceSkip, batchSize, writerQueueLen, writerCount, lagReportFreq : TimeSpan option, streams: string list) = async {
    //let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
    //    Log.Information("Lags by Range {@rangeLags}", remainingWork)
    //    return! Async.Sleep interval }
    //let maybeLogLag = lagReportFreq |> Option.map logLag
    let enumEvents (slice : AllEventsSlice) = seq {
        for e in slice.Events ->
            match e.Event with
            | e when e.IsJson -> Choice1Of2 (e.EventStreamId, e.EventNumber, Equinox.Cosmos.Store.EventData.Create(e.EventType, e.Data, e.Metadata))
            | e -> Choice2Of2 e
    }
    let mutable total = 0L
    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    let followAll (postBatch : Ingester.Batch -> unit) =
        let rec loop pos = async {
            let! currentSlice = source.ReadConnection.ReadAllEventsForwardAsync(pos, batchSize, resolveLinkTos = false) |> Async.AwaitTaskCorrect
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            let received = currentSlice.Events.Length
            total <- total + int64 received
            let streams =
                enumEvents currentSlice
                |> Seq.choose (function Choice1Of2 e -> Some e | Choice2Of2 _ -> None)
                |> Seq.groupBy (fun (s,_,_) -> s)
                |> Seq.map (fun (s,xs) -> s, [| for _s, i, e in xs -> i, e |])
                |> Array.ofSeq

            match streams |> Seq.map (fun (_s,xs) -> Array.length xs) |> Seq.sum with
            | extracted when extracted <> received -> Log.Warning("Dropped {count} of {total}", received-extracted, received)
            | _ -> ()

            let category (s : string) = s.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries) |> Array.head
            let cats = seq { for (s,_) in streams -> category s } |> Seq.distinct |> Seq.length
            let postSw = Stopwatch.StartNew()
            for s,xs in streams do
                for pos, item in xs do 
                    postBatch { stream = s; pos = pos; events = [| item |]}
            Log.Information("Fetch {count} {ft:n0}ms; Parse c {categories} s {streams}; Post {pt:n0}ms",
                received, sw.ElapsedMilliseconds, cats, streams.Length, postSw.ElapsedMilliseconds)
            if currentSlice.IsEndOfStream then Log.Warning("Completed {total:n0}", total)
            
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
            if not currentSlice.IsEndOfStream then return! loop currentSlice.NextPosition
        }
        fun pos -> loop pos

    //let fetchBatches stream pos size =
    //    let fetchFull pos = async {
    //        let! currentSlice = source.ReadConnection.ReadStreamEventsBackwardAsync(stream, pos, batchSize, resolveLinkTos=true) |> Async.AwaitTaskCorrect
    //        if currentSlice.IsEndOfStream then return None
    //        else return Some (currentSlice.NextEventNumber,currentSlice.Events)
    //    }
    //    AsyncSeq.unfoldAsync fetchFull 0L

    //let fetchSpecific streams = async {
    //    let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
    //    for stream in streams do
    //        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
    //        enumStreamEvents currentSlice
    //        |> Seq.choose (function Choice1Of2 e -> Some e | Choice2Of2 _ -> None)
    //        |> Seq.groupBy (fun (s,_,_) -> s)
    //        |> Seq.map (fun (s,xs) -> s, [| for _s, i, e in xs -> i, e |])
    //        |> Array.ofSeq
    //        |> xs.AddRange 
    //    sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
    //    if not currentSlice.IsEndOfStream then return! fetch currentSlice.NextPosition
            
    //}


    let ctx = Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.Logger)
    let! ingester = Ingester.start(ctx, writerQueueLen, writerCount)
    let! _ = Async.StartChild (Reader.loadSpecificStreamsTemp (source.ReadConnection, batchSize) streams ingester.Add)
    let! _ = Async.StartChild (followAll ingester.Add Position.Start)
    do! Async.AwaitKeyboardInterrupt() }
 
[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose
        let connectionMode = Equinox.EventStore.ConnectionStrategy.ClusterSingle Equinox.EventStore.NodePreference.Master
        let source = args.EventStore.Connect(Log.Logger, Log.Logger, connectionMode) |> Async.RunSynchronously
        let cosmos = args.EventStore.Cosmos // wierd nesting is due to me not finding a better way to express the semantics in Argu
        let destination = cosmos.Connnect "ProjectorTemplate" |> Async.RunSynchronously
        let colls = CosmosCollections(cosmos.Database, cosmos.Collection)
        let leaseId, startFromHere, batchSize, lagFrequency, streams = args.BuildFeedParams()
        let writerQueueLen, writerCount = 10, 2
        run (destination, colls) source (leaseId, startFromHere, batchSize, writerQueueLen, writerCount, lagFrequency, streams) |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1