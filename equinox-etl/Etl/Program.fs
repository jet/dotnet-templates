module EtlTemplate.Program

open Equinox.Cosmos
open Equinox.Cosmos.Projection
open Equinox.Store
open Microsoft.Azure.Documents.ChangeFeedProcessor
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading

module CmdParser =
    open Argu

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<MainCommand; ExactlyOnce>] ConsumerGroupName of string
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        | [<AltCommandLine "-s"; Unique>] LeaseCollectionSuffix of string
        | [<AltCommandLine "-i"; Unique>] ForceStartFromHere
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-l"; Unique>] LagFreqS of float
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Source of ParseResults<SourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | LeaseCollectionSuffix _ ->"specify Collection Name suffix for Leases collection, relative to `cosmos` arguments (default: `-aux`)."
                | ForceStartFromHere _ ->   "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | BatchSize _ ->            "maximum item count to request from feed. Default: 1000"
                | LagFreqS _ ->             "specify frequency to dump lag stats. Default: off"
                | Verbose ->                "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | Source _ ->               "CosmosDb input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.LeaseId =             a.GetResult ConsumerGroupName

        member __.Verbose =             a.Contains Verbose

        member __.Suffix =              a.GetResult(LeaseCollectionSuffix,"-aux")
        member __.ChangeFeedVerbose =   a.Contains ChangeFeedVerbose
        member __.BatchSize =           a.GetResult(BatchSize,1000)
        member __.LagFrequency =        a.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds
        member __.AuxCollectionName =   __.Destination.Collection + __.Suffix
        member __.StartFromHere =       a.Contains ForceStartFromHere

        member val Source = SourceArguments(a.GetResult Source)
        member __.Destination : DestinationArguments = __.Source.Destination
        member x.BuildChangeFeedParams() =
            Log.Information("Processing {leaseId} in {auxCollName} in batches of {batchSize}", x.LeaseId, x.AuxCollectionName, x.BatchSize)
            if x.StartFromHere then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            x.Destination.Discovery, { database = x.Destination.Database; collection = x.AuxCollectionName}, x.LeaseId, x.StartFromHere, x.BatchSize, x.LagFrequency
    and [<NoEquality; NoComparison>] SourceParameters =
        | [<AltCommandLine "-m">] SourceConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">] SourceTimeout of float
        | [<AltCommandLine "-r">] SourceRetries of int
        | [<AltCommandLine "-rt">] SourceRetriesWaitTime of int
        | [<AltCommandLine "-s"; Unique(*Mandatory is not supported*)>] SourceConnection of string
        | [<AltCommandLine "-d"; Unique(*Mandatory is not supported*)>] SourceDatabase of string
        | [<AltCommandLine "-c"; Unique(*Mandatory is not supported*)>] SourceCollection of string
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<DestinationParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | SourceTimeout _ ->        "specify operation timeout in seconds (default: 5)."
                | SourceRetries _ ->        "specify operation retries (default: 1)."
                | SourceRetriesWaitTime _ ->"specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | SourceConnection _ ->     "specify a connection string for a Cosmos account."
                | SourceConnectionMode _ -> "override the connection mode (default: DirectTcp)."
                | SourceDatabase _ ->       "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                | SourceCollection _ ->     "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
                | Cosmos _ ->               "CosmosDb destination parameters."
    and SourceArguments(a : ParseResults<SourceParameters>) =
        member val Destination =        DestinationArguments(a.GetResult Cosmos)
        member __.Mode =                a.GetResult(SourceConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Connection =          a.GetResult SourceConnection
        member __.Database =            a.GetResult SourceDatabase
        member __.Collection =          a.GetResult SourceCollection

        member __.Timeout =             a.GetResult(SourceTimeout,5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(SourceRetries, 1)
        member __.MaxRetryWaitTime =    a.GetResult(SourceRetriesWaitTime, 5)

        member x.BuildConnectionDetails() =
            let (Discovery.UriAndKey (endpointUri,_)) as discovery = Discovery.FromConnectionString x.Connection
            Log.Information("Source CosmosDb {mode} {endpointUri} Database {database} Collection {collection}",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("Source CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            discovery, { database = x.Database; collection = x.Collection }, c.ConnectionPolicy
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
                | Connection _ ->           "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION)."
                | Database _ ->             "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE)."
                | Collection _ ->           "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION)."
                | Timeout _ ->              "specify operation timeout in seconds (default: 5)."
                | Retries _ ->              "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->       "override the connection mode (default: DirectTcp)."
    and DestinationArguments(a : ParseResults<DestinationParameters>) =
        member __.Mode =                a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"
        member __.Discovery =           Discovery.FromConnectionString __.Connection

        member __.Timeout =             a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
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
            |> fun c -> let ol = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<ChangeFeedObserver>.FullName, ol)
            |> fun c -> let cl = if verbose then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
                        c.MinimumLevel.Override("Equinox.Cosmos.Core.CosmosContext", cl)
            |> fun c -> c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
            |> fun c -> c.CreateLogger()
        Log.ForContext<ChangeFeedObserver>()

module Ingester =
    open Equinox.Cosmos.Core
    open Equinox.Cosmos.Store

    type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
    type [<NoComparison>] Batch = { stream: string; span: Span }

    module Writer =
        type [<NoComparison>] Result =
            | Ok of stream: string * updatedPos: int64
            | Duplicate of stream: string * updatedPos: int64
            | Conflict of overage: Batch
            | Exn of exn: exn * batch: Batch
            member __.WriteTo(log: ILogger) =
                match __ with
                | Ok (stream, pos) ->           log.Information("Wrote     {stream} up to {pos}", stream, pos)
                | Duplicate (stream, pos) ->    log.Information("Ignored   {stream} (synced up to {pos})", stream, pos)
                | Conflict overage ->           log.Information("Requeing  {stream} {pos} ({count} events)", overage.stream, overage.span.index, overage.span.events.Length)
                | Exn (exn, batch) ->           log.Warning(exn,"Writing   {stream} failed, retrying {count} events ....", batch.stream, batch.span.events.Length)
        let write (log : ILogger) (ctx : CosmosContext) ({ stream = s; span = { index = p; events = e}} as batch) = async {
            let stream = ctx.CreateStream s
            log.Information("Writing {s}@{i}x{n}",s,p,e.Length)
            try let! res = ctx.Sync(stream, { index = p; etag = None }, e)
                match res with
                | AppendResult.Ok pos -> return Ok (s, pos.index) 
                | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                    match pos.index, p + e.LongLength with
                    | actual, expectedMax when actual >= expectedMax -> return Duplicate (s, pos.index)
                    | actual, _ when p >= actual -> return Conflict batch
                    | actual, _ ->
                        log.Information("pos {pos} batch.pos {bpos} len {blen} skip {skip}", actual, p, e.LongLength, actual-p)
                        return Conflict { stream = s; span = { index = actual; events = e |> Array.skip (actual-p |> int) } }
            with e -> return Exn (e, batch) }

    module Queue =
        module Span =
            let private (|Max|) x = x.index + x.events.LongLength
            let private trim min (Max m as x) =
                // Full remove
                if m <= min then { index = min; events = [||] }
                // Trim until min
                elif m > min && x.index < min then { index = min; events = x.events |> Array.skip (min - x.index |> int) }
                // Leave it
                else x
            let merge min (xs : Span seq) =
                let buffer = ResizeArray()
                let mutable curr = { index = min; events = [||]}
                for x in xs |> Seq.sortBy (fun x -> x.index) do
                    match curr, trim min x with
                    // no data incoming, skip
                    | _, x when x.events.Length = 0 ->
                        ()
                    // Not overlapping, no data buffered -> buffer
                    | c, x when c.events.Length = 0 ->
                        curr <- x
                    // Overlapping, join
                    | Max cMax as c, x when cMax >= x.index ->
                        curr <- { c with events = Array.append c.events (trim cMax x).events }
                    // Not overlapping, new data
                    | c, x ->
                        buffer.Add c
                        curr <- x
                if curr.events.Length <> 0 then buffer.Add curr
                if buffer.Count = 0 then null else buffer.ToArray()

        type [<NoComparison>] StreamState = { write: int64; queue: Span[] }
        module StreamState =
            let combine (s1: StreamState) (s2: StreamState) : StreamState =
                let writePos = max s1.write s2.write
                let items = seq { if s1.queue <> null then yield! s1.queue; if s2.queue <> null then yield! s2.queue }
                { write = writePos; queue = Span.merge writePos items}

        type StreamStates() =
            let states = System.Collections.Generic.Dictionary<string, StreamState>()
            
            let update stream (state : StreamState) =
                match states.TryGetValue stream with
                | false, _ ->
                    states.Add(stream, state)
                | true, current ->
                    let updated = StreamState.combine current state
                    states.[stream] <- updated
            let updateWritePos stream pos span =
                update stream { write = pos; queue = span }

            member __.Add (item: Batch) = updateWritePos item.stream 0L [|item.span|]
            member __.HandleWriteResult = function
                | Writer.Result.Ok (stream, pos) -> updateWritePos stream pos null
                | Writer.Result.Duplicate (stream, pos) -> updateWritePos stream pos null
                | Writer.Result.Conflict overage -> updateWritePos overage.stream overage.span.index [|overage.span|]
                | Writer.Result.Exn (_exn, batch) -> __.Add(batch)

            member __.PendingBatches =
                [| for KeyValue (stream, state) in states do
                    if state.queue <> null then
                        let x = state.queue |> Array.head
                        let mutable count = 0 
                        let max1000EventsMax10EventsFirstTranche (y : Equinox.Codec.IEvent<byte[]>) =
                            count <- count + 1
                            // Reduce the item count when we don't yet know the write position
                            count <= (if state.write = 0L then 10 else 1000)
                        yield { stream = stream; span = { index = x.index; events = x.events |> Array.takeWhile max1000EventsMax10EventsFirstTranche } } |]

    /// Manages distribution of work across a specified number of concurrent writers
    type SynchronousWriter(ctx : CosmosContext, log, ?maxDop) =
        let states = Queue.StreamStates()
        let dop = new SemaphoreSlim(defaultArg maxDop 10)
        member __.Add item = states.Add item
        member __.Pump(?attempts) = async {
            let attempts = defaultArg attempts 5
            let rec loop pending remaining = async {
                let! results = Async.Parallel <| seq { for batch in pending -> Writer.write log ctx batch |> dop.Throttle }
                let mutable retriesRequired = false
                for res in results do
                    states.HandleWriteResult res
                    res.WriteTo log
                    match res with
                    | Writer.Result.Ok _ | Writer.Result.Duplicate _ -> ()
                    | Writer.Result.Conflict _ | Writer.Result.Exn _ -> retriesRequired <- true
                let pending = states.PendingBatches
                if not <| Array.isEmpty pending then
                    if retriesRequired then Log.Warning("Retrying; {count} streams to sync", Array.length pending)
                    else log.Information("Syncing {count} streams", Array.length pending)
                    if remaining <= 1 then log.Error("{retries} Sync attempts exceeded", attempts); failwith "Sync failed"
                    else return! loop pending (remaining-1) }
            let pending = states.PendingBatches
            let streams = Array.length pending
            log.Information("Syncing {count} streams", streams)
            do! loop pending attempts
            return streams }

let run (sourceDiscovery, source) (auxDiscovery, aux) connectionPolicy (leaseId, forceSkip, batchSize, lagReportFreq : TimeSpan option)
        createRangeProjector = async {
    let logLag (interval : TimeSpan) (remainingWork : (int*int64) seq) = async {
        Log.Information("Lags by Range {@rangeLags}", remainingWork)
        return! Async.Sleep interval }
    let maybeLogLag = lagReportFreq |> Option.map logLag
    let! _feedEventHost =
        ChangeFeedProcessor.Start
          ( Log.Logger, sourceDiscovery, connectionPolicy, source, aux, auxDiscovery = auxDiscovery, leasePrefix = leaseId, forceSkipExistingEvents = forceSkip,
            cfBatchSize = batchSize, createObserver = createRangeProjector, ?reportLagAndAwaitNextEstimation = maybeLogLag)
    do! Async.AwaitKeyboardInterrupt() }

let createRangeSyncHandler log (ctx: Core.CosmosContext) (transform : Microsoft.Azure.Documents.Document -> Ingester.Batch seq) =
    let sw = Stopwatch.StartNew() // we'll end up reporting the warmup/connect time on the first batch, but that's ok
    let writer = Ingester.SynchronousWriter(ctx, log)
    let processBatch (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
        sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
        let pt, events = Stopwatch.Time (fun () ->
            let items = docs |> Seq.collect transform |> Array.ofSeq
            items |> Array.iter writer.Add
            items)
        let! et,streams = writer.Pump() |> Stopwatch.Time
        let r = ctx.FeedResponse
        Log.Information("Read {range,2} -{token,6} {count,4} docs {requestCharge,6}RU {l:n1}s Gen {events,5} events {p:n3}s Sync {streams,5} streams {e:n1}s",
            ctx.PartitionKeyRangeId, r.ResponseContinuation.Trim[|'"'|], docs.Count, (let c = r.RequestCharge in c.ToString("n1")),
            float sw.ElapsedMilliseconds / 1000., events.Length, (let e = pt.Elapsed in e.TotalSeconds), streams, (let e = et.Elapsed in e.TotalSeconds))
        sw.Restart() // restart the clock as we handoff back to the ChangeFeedProcessor
    }
    ChangeFeedObserver.Create(log, processBatch)

open Microsoft.Azure.Documents

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
    /// Infers whether the document represents a valid Event-Batch
    let parse (d : Document) =
        //if d.GetPropertyValue("p") <> null && d.GetPropertyValue("i") <> null
        //   && d.GetPropertyValue("n") <> null && d.GetPropertyValue("e") <> null then
        let x = d.Cast<EventV0>()
        { new IEvent with
              member __.Index = x.i
              member __.IsUnfold = false
              member __.EventType = x.t
              member __.Data = x.d
              member __.Meta = null
              member __.Timestamp = x.c
              member __.Stream = x.s }

let transformV0 (v0SchemaDocument: Document) : Ingester.Batch seq = seq {
    let parsed = EventV0Parser.parse v0SchemaDocument
    let streamName = if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+parsed.Stream
    yield { stream = streamName; span = { index = parsed.Index; events = [| parsed |] } } }
//#else
let transform (changeFeedDocument: Document) : Ingester.Batch seq = seq {
    (* TODO MAKE THIS DO YOUR BIDDING *)
    for e in DocumentParser.enumEvents changeFeedDocument ->
        let streamName = "Cloned-" + e.Stream
        { stream = streamName; span = { index = e.Index; events = [| e |] } }
}
//#endif

[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        let log = Logging.initialize args.Verbose args.ChangeFeedVerbose
        let discovery, source, connectionPolicy = args.Source.BuildConnectionDetails()
        let target =
            let destination = args.Destination.Connect "EtlTemplate" |> Async.RunSynchronously
            let colls = CosmosCollections(args.Destination.Database, args.Destination.Collection)
            Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.ForContext<Core.CosmosContext>())
        let auxDiscovery, aux, leaseId, startFromHere, batchSize, lagFrequency = args.BuildChangeFeedParams()
#if marveleqx
        let createSyncHandler () = createRangeSyncHandler log target transformV0
#else
        let createSyncHandler () = createRangeSyncHandler log target transform
#endif
//#if Testing
        let createSyncHandler () = createRangeSyncHandler log target transformV0
//#endif
        run (discovery, source) (auxDiscovery, aux) connectionPolicy
            (leaseId, startFromHere, batchSize, lagFrequency)
            createSyncHandler
        |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1