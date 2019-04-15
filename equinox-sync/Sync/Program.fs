module SyncTemplate.Program

open Equinox.Cosmos
open Equinox.Cosmos.Core
open Equinox.Cosmos.Projection
open Equinox.Store
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading

let mb x = float x / 1024. / 1024.
let category (streamName : string) = streamName.Split([|'-'|],2).[0]
let every ms f =
    let timer = Stopwatch.StartNew()
    fun () ->
        if timer.ElapsedMilliseconds > ms then
            f ()
            timer.Restart()

module CmdParser =
    open Argu

    exception InvalidArguments of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| InvalidArguments (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine "-S"; Unique>] LocalSeq
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSource of string
        | [<AltCommandLine "-ad"; Unique>] LeaseCollectionDestination of string
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        | [<AltCommandLine "-f"; Unique>] ForceStartFromHere
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Source of ParseResults<SourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | ForceStartFromHere _ ->   "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 1000"
                | LeaseCollectionSource _ ->"specify Collection Name for Leases collection, within `source` connection/database (default: `source`'s `collection` + `-aux`)."
                | LeaseCollectionDestination _ -> "specify Collection Name for Leases collection, within [destination] `cosmos` connection/database (default: defined relative to `source`'s `collection`)."
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | Source _ ->               "CosmosDb input parameters."
                | Verbose ->                "request Verbose Logging. Default: off"
    and Arguments(a : ParseResults<Parameters>) =
        member __.MaybeSeqEndpoint =    if a.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.LeaseId =             a.GetResult ConsumerGroupName
        member __.BatchSize =           a.GetResult(BatchSize,1000)
        member __.StartFromHere =       a.Contains ForceStartFromHere
        member __.LagFrequency =        a.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.ChangeFeedVerbose =   a.Contains ChangeFeedVerbose

        member __.Verbose =             a.Contains Verbose

        member val Source : SourceArguments = SourceArguments(a.GetResult Source)
        member __.Destination : DestinationArguments = __.Source.Destination
        member x.BuildChangeFeedParams() =
            let disco, db =
                match a.TryGetResult LeaseCollectionSource, a.TryGetResult LeaseCollectionDestination with
                | None, None ->     x.Source.Discovery, { database = x.Source.Database; collection = x.Source.Collection + "-aux" }
                | Some sc, None ->  x.Source.Discovery, { database = x.Source.Database; collection = sc }
                | None, Some dc ->  x.Destination.Discovery, { database = x.Destination.Database; collection = dc }
                | Some _, Some _ -> raise (InvalidArguments "LeaseCollectionSource and LeaseCollectionDestination are mutually exclusive - can only store in one database")
            Log.Information("Processing Lease {leaseId} in Database {db} Collection {coll} in batches of {batchSize}", x.LeaseId, db.database, db.collection, x.BatchSize)
            if x.StartFromHere then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            disco, db, x.LeaseId, x.StartFromHere, x.BatchSize, x.LagFrequency
    and [<NoEquality; NoComparison>] SourceParameters =
        | [<AltCommandLine "-m">] SourceConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">] SourceTimeout of float
        | [<AltCommandLine "-r">] SourceRetries of int
        | [<AltCommandLine "-rt">] SourceRetriesWaitTime of int
        | [<AltCommandLine "-s">] SourceConnection of string
        | [<AltCommandLine "-d">] SourceDatabase of string
        | [<AltCommandLine "-c"; Unique(*Mandatory is not supported*)>] SourceCollection of string
        | [<AltCommandLine "-e">] CategoryBlacklist of string
        | [<AltCommandLine "-i">] CategoryWhitelist of string
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<DestinationParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | SourceConnection _ ->     "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION)."
                | SourceDatabase _ ->       "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE)."
                | SourceCollection _ ->     "specify a collection name within `SourceDatabase`."
                | SourceTimeout _ ->        "specify operation timeout in seconds (default: 5)."
                | SourceRetries _ ->        "specify operation retries (default: 1)."
                | SourceRetriesWaitTime _ ->"specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | SourceConnectionMode _ -> "override the connection mode (default: DirectTcp)."
                | CategoryBlacklist _ ->    "Category whitelist"
                | CategoryWhitelist _ ->    "Category blacklist"
                | Cosmos _ ->               "CosmosDb destination parameters."
    and SourceArguments(a : ParseResults<SourceParameters>) =
        member val Destination =        DestinationArguments(a.GetResult Cosmos)
        member __.CategoryFilterFunction : string -> bool =
            match a.GetResults CategoryBlacklist, a.GetResults CategoryWhitelist with
            | [], [] ->     Log.Information("Not filtering by category"); fun _ -> true 
            | bad, [] ->    let black = Set.ofList bad in Log.Information("Excluding categories: {cats}", black); fun x -> not (black.Contains x)
            | [], good ->   let white = Set.ofList good in Log.Information("Only copying categories: {cats}", white); fun x -> white.Contains x
            | _, _ -> raise (InvalidArguments "BlackList and Whitelist are mutually exclusive; inclusions and exclusions cannot be mixed")
        member __.Mode =                a.GetResult(SourceConnectionMode, Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Discovery =           Discovery.FromConnectionString __.Connection
        member __.Connection =          match a.TryGetResult SourceConnection   with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult SourceDatabase     with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          a.GetResult SourceCollection

        member __.Timeout =             a.GetResult(SourceTimeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(SourceRetries, 1)
        member __.MaxRetryWaitTime =    a.GetResult(SourceRetriesWaitTime, 5)
        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri,_)) as discovery = x.Discovery
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Collection {collection}",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; collection = x.Collection }, c.ConnectionPolicy, x.CategoryFilterFunction
    and [<NoEquality; NoComparison>] DestinationParameters =
        | [<AltCommandLine("-s")>] Connection of string
        | [<AltCommandLine("-d")>] Database of string
        | [<AltCommandLine("-c")>] Collection of string
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-rt")>] RetriesWaitTime of int
        | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | Connection _ ->           "specify a connection string for a Cosmos account (default: envvar:EQUINOX_COSMOS_CONNECTION)."
                | Database _ ->             "specify a database name for Cosmos account (default: envvar:EQUINOX_COSMOS_DATABASE)."
                | Collection _ ->           "specify a collection name for Cosmos account (default: envvar:EQUINOX_COSMOS_COLLECTION)."
                | Timeout _ ->              "specify operation timeout in seconds (default: 5)."
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->       "override the connection mode (default: DirectTcp)."
    and DestinationArguments(a : ParseResults<DestinationParameters>) =
        member __.Mode =                a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Discovery =           Discovery.FromConnectionString __.Connection
        member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

        member __.Timeout =             a.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries, 1)
        member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5)

        /// Connect with the provided parameters and/or environment variables
        member x.Connect
            /// Connection/Client identifier for logging purposes
            name : Async<CosmosConnection> =
            let (Discovery.UriAndKey (endpointUri,_masterKey)) as discovery = x.Discovery
            Log.Information("Destination CosmosDb {mode} {endpointUri} Database {database} Collection {collection}",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("Destination CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c.Connect(name, discovery)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

module CosmosSource =
    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing

    type [<RequireQualifiedAccess; NoEquality; NoComparison>] CoordinationWork<'Pos> =
        | Result of CosmosIngester.Writer.Result
        | ProgressResult of Choice<int,exn>
        | BatchWithTracking of 'Pos * CosmosIngester.Batch[]

    /// Manages writing of progress
    /// - Each write attempt is of the newest token
    /// - retries until success or a new item is posted
    type ProgressWriter() =
        let pumpSleepMs = 100
        let mutable lastCompleted = -1
        let mutable latest = None
        let result = Event<_>()
        [<CLIEvent>] member __.Result = result.Publish
        member __.Post(version,f) = latest <- Some (version,f) 
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                match latest with
                | Some (v,f) when v <> lastCompleted ->
                    try do! f
                        lastCompleted <- v
                        result.Trigger (Choice1Of2 v)
                    with e -> result.Trigger (Choice2Of2 e)
                | _ -> do! Async.Sleep pumpSleepMs }
    type PendingWork = { batches : int; streams : int }
    type Coordinator private (cosmosContext, cts : CancellationTokenSource, ?maxWriters, ?interval) =
        let pumpSleepMs = 100
        let maxWriters = defaultArg maxWriters 256
        let statsIntervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
        let work = System.Collections.Concurrent.ConcurrentQueue()
        let buffer = CosmosIngester.StreamStates()
        let progressWriter = ProgressWriter()
        let syncState = ProgressBatcher.State()
        let mutable epoch = 0
        let postBatch pos xs =
            let batchStamp = Interlocked.Increment &epoch
            work.Enqueue(CoordinationWork.BatchWithTracking ((batchStamp,pos),xs))
        let postWriteResult = work.Enqueue << CoordinationWork.Result
        let postWriteProgressResult = work.Enqueue << CoordinationWork.ProgressResult
        let mutable pendingBatches = 0
        member __.Pump(log : ILogger) = async {
            let writers = CosmosIngester.Writers(CosmosIngester.Writer.write log cosmosContext, maxWriters)
            let writerResultLog = log.ForContext<CosmosIngester.Writer.Result>()
            use _ = writers.Result.Subscribe postWriteResult
            use _ = progressWriter.Result.Subscribe postWriteProgressResult
            let! _ = Async.StartChild <| writers.Pump()
            let! _ = Async.StartChild <| progressWriter.Pump()
            let! ct = Async.CancellationToken
            let mutable bytesPended = 0L
            let resultsHandled, workPended, eventsPended = ref 0, ref 0, ref 0
            let rateLimited, timedOut, malformed = ref 0, ref 0, ref 0
            let progressFailed, progressWritten = ref 0, ref 0
            let mutable progressEpoch = None
            let badCats = CosmosIngester.CatStats()
            let dumpStats () =
                if !rateLimited <> 0 || !timedOut <> 0 || !malformed <> 0 then
                    log.Warning("Writer Exceptions {rateLimited} rate-limited, {timedOut} timed out, {malformed} malformed",!rateLimited, !timedOut, !malformed)
                    rateLimited := 0; timedOut := 0; malformed := 0 
                    if badCats.Any then log.Error("Malformed categories {badCats}", badCats.StatsDescending); badCats.Clear()
                let tl = if !workPended = 0 && !eventsPended = 0 && !resultsHandled = 0 then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                log.Write(tl, "Writer Throughput {queued} requests, {events} events; Completed {completed} reqs; Egress {gb:n3}GB",
                    !workPended, !eventsPended,!resultsHandled, mb bytesPended / 1024.)
                resultsHandled := 0; workPended := 0; eventsPended := 0 
                if !progressFailed <> 0 || !progressWritten <> 0 then
                    match progressEpoch with
                    | None -> log.Error("Progress writing failing: {failures} failures", !progressWritten, !progressFailed)
                    | Some epoch ->
                        if !progressFailed <> 0 then log.Warning("Progress Epoch {epoch} ({updates} updates, {failures} failures", epoch, !progressWritten, !progressFailed)
                        else log.Information("Progress Epoch {epoch} ({updates} updates)", epoch, !progressWritten)
                    progressFailed := 0; progressWritten := 0
                buffer.Dump log
            let tryDumpStats = every statsIntervalMs dumpStats
            let handle = function
                | CoordinationWork.BatchWithTracking(pos, items) ->
                    for item in items do
                        buffer.Add item |> ignore
                    syncState.AppendBatch(pos, [|for x in items -> x.stream, x.span.index + int64 x.span.events.Length |])
                | CoordinationWork.ProgressResult (Choice1Of2 epoch) ->
                    incr progressWritten
                    progressEpoch <- Some epoch
                | CoordinationWork.ProgressResult (Choice2Of2 (_exn : exn)) ->
                    incr progressFailed
                | CoordinationWork.Result res ->
                    incr resultsHandled
                    let (stream, updatedState), kind = buffer.HandleWriteResult res
                    match updatedState.write with None -> () | Some wp -> syncState.MarkStreamProgress(stream, wp)
                    match kind with
                    | CosmosIngester.Ok -> res.WriteTo writerResultLog
                    | CosmosIngester.RateLimited -> incr rateLimited
                    | CosmosIngester.Malformed -> category stream |> badCats.Ingest; incr malformed
                    | CosmosIngester.TimedOut -> incr timedOut
            let queueWrite (w : CosmosIngester.Batch) =
                incr workPended
                eventsPended := !eventsPended + w.span.events.Length
                bytesPended <- bytesPended + int64 (Array.sumBy CosmosIngester.cosmosPayloadBytes w.span.events)
                writers.Enqueue w
            while not ct.IsCancellationRequested do
                // 1. propagate read items to buffer; propagate write results to buffer + Progress 
                match work.TryDequeue() with
                | true, item ->
                    handle item
                | false, _ ->
                    let validatedPos, pendingBatchCount = syncState.Validate buffer.TryGetStreamWritePos
                    pendingBatches <- pendingBatchCount
                    // 2. After that, [over] provision writers queue
                    let mutable more = writers.HasCapacity
                    while more do
                        match buffer.TryReady(writers.IsStreamBusy) with
                        | Some w -> queueWrite w; more <- writers.HasCapacity
                        | None -> more <- false
                    // 3. Periodically emit status info
                    tryDumpStats ()
                    // 4. Feed latest state to progress writer
                    validatedPos |> Option.iter progressWriter.Post
                    // 5. Sleep if we've nothing else to do
                    do! Async.Sleep pumpSleepMs
            dumpStats ()
            log.Warning("... Coordinator exiting") }

        static member Start(log : Serilog.ILogger, cosmosContext) : Coordinator =
            let cts = new CancellationTokenSource()
            let coordinator = new Coordinator(cosmosContext, cts)
            Async.Start(coordinator.Pump log,cts.Token)
            coordinator
        member __.Submit(checkpoint : Async<unit>, batches : CosmosIngester.Batch[]) = postBatch checkpoint batches
        member __.PendingBatches = pendingBatches
        interface IDisposable with member __.Dispose() = cts.Cancel()

    let createRangeSyncHandler (log:ILogger) (ctx: Core.CosmosContext) (transform : Microsoft.Azure.Documents.Document -> CosmosIngester.Batch seq) =
        let busyPauseMs = 500
        let maxUnconfirmedBatches = 10
        let mutable coordinator = Unchecked.defaultof<_>
        let init rangeLog =
            coordinator <- Coordinator.Start(rangeLog, ctx)
        let ingest docs checkpoint : (*streams*)int * (*events*)int =
            let events = docs |> Seq.collect transform |> Array.ofSeq
            coordinator.Submit(checkpoint,events)
            events.Length, HashSet(seq { for x in events -> x.stream }).Count
        let dispose () = (coordinator :> IDisposable).Dispose()
        let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
        let processBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
            sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
            // Pass along the function that the coordinator will run to checkpoint past this batch when such progress has been achieved
            let checkpoint = async { do! ctx.CheckpointAsync() |> Async.AwaitTaskCorrect }
            let pt, (events,streams) = Stopwatch.Time(fun () -> ingest docs checkpoint)
            log.Information("Read -{token,6} {count,4} docs {requestCharge,6}RU {l:n1}s Gen {events,5} events {p:n3}s Sync {streams,5} streams",
                ctx.FeedResponse.ResponseContinuation.Trim[|'"'|], docs.Count, (let c = ctx.FeedResponse.RequestCharge in c.ToString("n1")),
                float sw.ElapsedMilliseconds / 1000., events, (let e = pt.Elapsed in e.TotalSeconds), streams)
            // Only hand back control to the CFP iff our processing backlog is under control
            // no point getting too far ahead and/or overloading ourselves if we can't log our progress
            let mutable first = true
            let pauseTimer = Stopwatch.StartNew()
            let backlogIsExcessive pendingBatches =
                let tooMuch = pendingBatches >= maxUnconfirmedBatches
                if first && tooMuch then log.Information("Pausing due to backlog of incomplete batches...")
                let longDelay = pauseTimer.ElapsedMilliseconds > 5000L
                let level = if tooMuch && (first || longDelay) then Events.LogEventLevel.Information else Events.LogEventLevel.Debug
                first <- false
                if longDelay then pauseTimer.Reset()
                log.Write(level, "Pending Batches {pb}", pendingBatches)
                tooMuch
            while backlogIsExcessive coordinator.PendingBatches do
                do! Async.Sleep busyPauseMs
            sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
        }
        ChangeFeedObserver.Create(log, processBatch, assign=init, dispose=dispose)

    let run (sourceDiscovery, source) (auxDiscovery, aux) connectionPolicy (leaseId, forceSkip, batchSize, lagReportFreq : TimeSpan option)
            createRangeProjector = async {
        let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
            Log.Information("Lags {@rangeLags} (Range, Docs count)", remainingWork)
            return! Async.Sleep interval }
        let maybeLogLag = lagReportFreq |> Option.map logLag
        let! _feedEventHost =
            ChangeFeedProcessor.Start
              ( Log.Logger, sourceDiscovery, connectionPolicy, source, aux, auxDiscovery = auxDiscovery, leasePrefix = leaseId, forceSkipExistingEvents = forceSkip,
                cfBatchSize = batchSize, createObserver = createRangeProjector, ?reportLagAndAwaitNextEstimation = maybeLogLag)
        do! Async.AwaitKeyboardInterrupt() }

    //#if marveleqx
    [<RequireQualifiedAccess>]
    module EventV0Parser =
        open Newtonsoft.Json

        /// A single Domain Event as Written by internal Equinox versions
        type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
            EventV0 =
            {   /// DocDb-mandated Partition Key, must be maintained within the document
                s: string // "{streamName}"

                /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
                c: DateTimeOffset // ISO 8601

                /// The Case (Event Type); used to drive deserialization
                t: string // required

                /// 'i' value for the Event
                i: int64 // {index}

                /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
                [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
                d: byte[] }

        type Document with
            member document.Cast<'T>() =
                let tmp = new Document()
                tmp.SetPropertyValue("content", document)
                tmp.GetPropertyValue<'T>("content")
        type IEvent =
            inherit Equinox.Codec.Core.IIndexedEvent<byte[]>
                abstract member Stream : string
        /// We assume all Documents represent Events laid out as above
        let parse (d : Document) =
            let x = d.Cast<EventV0>()
            { new IEvent with
                  member __.Index = x.i
                  member __.IsUnfold = false
                  member __.EventType = x.t
                  member __.Data = x.d
                  member __.Meta = null
                  member __.Timestamp = x.c
                  member __.Stream = x.s }

    let transformV0 catFilter (v0SchemaDocument: Document) : CosmosIngester.Batch seq = seq {
        let parsed = EventV0Parser.parse v0SchemaDocument
        let streamName = (*if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+*)parsed.Stream
        if catFilter (category streamName) then
            yield { stream = streamName; span = { index = parsed.Index; events = [| parsed |] } } }
    //#else
    let transformOrFilter catFilter (changeFeedDocument: Document) : CosmosIngester.Batch seq = seq {
        for e in DocumentParser.enumEvents changeFeedDocument do
            if catFilter (category e.Stream) then
                // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
                yield { stream = e.Stream; span = { index = e.Index; events = [| e |] } } }
    //#endif

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose changeLogVerbose maybeSeqEndpoint =
        Log.Logger <-
            LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
            |> fun c -> let cfpl = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
            |> fun c -> let ol = if changeLogVerbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Information
                        c.MinimumLevel.Override(typeof<CosmosIngester.Writers>.FullName, ol)
            |> fun c -> let ol = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<CosmosIngester.Writer.Result>.FullName, ol)
            |> fun c -> let cl = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<CosmosContext>.FullName, cl)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
            |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            |> fun c -> c.CreateLogger()
        Log.ForContext<CosmosIngester.Writers>()

[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        let target =
            let destination = args.Destination.Connect "SyncTemplate" |> Async.RunSynchronously
            let colls = CosmosCollections(args.Destination.Database, args.Destination.Collection)
            Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.ForContext<Core.CosmosContext>())
        let log = Logging.initialize args.Verbose args.ChangeFeedVerbose args.MaybeSeqEndpoint
        let discovery, source, connectionPolicy, catFilter = args.Source.BuildConnectionDetails()
        let auxDiscovery, aux, leaseId, startFromHere, batchSize, lagFrequency = args.BuildChangeFeedParams()
#if marveleqx
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#else
        let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformOrFilter catFilter)
        // Uncomment to test marveleqx mode
        // let createSyncHandler () = CosmosSource.createRangeSyncHandler log target (CosmosSource.transformV0 catFilter)
#endif
        CosmosSource.run (discovery, source) (auxDiscovery, aux) connectionPolicy
            (leaseId, startFromHere, batchSize, lagFrequency)
            createSyncHandler
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.InvalidArguments msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1