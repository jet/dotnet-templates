module SyncTemplate.Program

open Equinox.Store // Infra
open FSharp.Control
open Serilog
open System

type StartPos = Absolute of int64 | Percentage of float | Start
type ReaderSpec = { start: StartPos; batchSize: int; streams: string list }

module CmdParser =
    open Argu
    type LogEventLevel = Serilog.Events.LogEventLevel

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    module Cosmos =
        open Equinox.Cosmos
        type [<NoEquality; NoComparison>] Arguments =
            | [<AltCommandLine("-m")>] ConnectionMode of ConnectionMode
            | [<AltCommandLine("-o")>] Timeout of float
            | [<AltCommandLine("-r")>] Retries of int
            | [<AltCommandLine("-rt")>] RetriesWaitTime of int
            | [<AltCommandLine("-s")>] Connection of string
            | [<AltCommandLine("-d")>] Database of string
            | [<AltCommandLine("-c")>] Collection of string
            interface IArgParserTemplate with
                member a.Usage =
                    match a with
                    | Timeout _ ->          "specify operation timeout in seconds (default: 10)."
                    | Retries _ ->          "specify operation retries (default: 0)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                    | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                    | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                    | Database _ ->         "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                    | Collection _ ->       "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
        type Info(args : ParseResults<Arguments>) =
            member __.Connection =  match args.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
            member __.Database =    match args.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
            member __.Collection =  match args.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

            member __.Timeout = args.GetResult(Timeout,10.) |> TimeSpan.FromSeconds
            member __.Mode = args.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
            member __.Retries = args.GetResult(Retries, 0) 
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
            | [<AltCommandLine("-x")>] Port of int
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
                    | Timeout _ ->          "specify operation timeout in seconds (default: 20)."
                    | Retries _ ->          "specify operation retries (default: 3)."
                    | Host _ ->             "specify a DNS query, using Gossip-driven discovery against all A records returned (defaults: envvar:EQUINOX_ES_HOST, localhost)."
                    | Port _ ->             "specify a custom port (default: envvar:EQUINOX_ES_PORT, 30778)."
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
            member __.Port =     match args.TryGetResult Port       with Some x -> Some x | None -> Environment.GetEnvironmentVariable "EQUINOX_ES_PORT" |> Option.ofObj |> Option.map int
            member __.User =     match args.TryGetResult Username   with Some x -> x | None -> envBackstop "Username"   "EQUINOX_ES_USERNAME"
            member __.Password = match args.TryGetResult Password   with Some x -> x | None -> envBackstop "Password"   "EQUINOX_ES_PASSWORD"
            member val CacheStrategy = let c = Caching.Cache("ProjectorTemplate", sizeMb = 50) in CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            member __.Connect(log: ILogger, storeLog, connection) =
                let (timeout, retries) as operationThrottling = args.GetResult(Timeout,20.) |> TimeSpan.FromSeconds, args.GetResult(Retries,3)
                let heartbeatTimeout = args.GetResult(HeartbeatTimeout,1.5) |> TimeSpan.FromSeconds
                let concurrentOperationsLimit = args.GetResult(ConcurrentOperationsLimit,5000)
                log.Information("EventStore {host} heartbeat: {heartbeat}s MaxConcurrentRequests {concurrency} Timeout: {timeout}s Retries {retries}",
                    __.Host, heartbeatTimeout.TotalSeconds, concurrentOperationsLimit, timeout.TotalSeconds, retries)
                let discovery = match __.Port with None -> Discovery.GossipDns __.Host | Some p -> Discovery.GossipDnsCustomPort (__.Host, p)
                connect storeLog (heartbeatTimeout, concurrentOperationsLimit) operationThrottling discovery (__.User,__.Password) connection

    [<NoEquality; NoComparison>]
    type Arguments =
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-vc"; Unique>] VerboseConsole
        | [<AltCommandLine "-S"; Unique>] LocalSeq
        | [<AltCommandLine "-s">] Stream of string
        | [<AltCommandLine "-o"; Unique>] Offset of int64
        | [<AltCommandLine "-P"; Unique>] Percent of float
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Es of ParseResults<EventStore.Arguments>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | Verbose ->                "request Verbose Logging. Default: off"
                | VerboseConsole ->         "request Verbose Console Logging. Default: off"
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | Stream _ ->               "specific stream(s) to read"
                | Offset _ ->               "EventStore $all Stream Position to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"
                | Es _ ->                   "specify EventStore parameters"
    and Parameters(args : ParseResults<Arguments>) =
        member val EventStore = EventStore.Info(args.GetResult Es)
        member __.Verbose = args.Contains Verbose
        member __.ConsoleMinLevel = if args.Contains VerboseConsole then LogEventLevel.Information else LogEventLevel.Warning
        member __.MaybeSeqEndpoint = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.BatchSize = args.GetResult(BatchSize,4096)
        member x.BuildFeedParams() : ReaderSpec =
            Log.Information("Processing in batches of {batchSize}", x.BatchSize)
            let startPos =
                match args.TryGetResult Offset, args.TryGetResult Percent with
                | Some p, _ ->  Log.Warning("Processing will commence at $all Position {p}", p); Absolute p
                | _, Some p ->  Log.Warning("Processing will commence at $all Percentage {pct:P}", p/100.); Percentage p 
                | None, None -> Log.Warning "Processing will commence at $all Start"; Start
            { start = startPos; batchSize = x.BatchSize; streams = args.GetResults Stream }

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Parameters =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Arguments>(programName = programName)
        parser.ParseCommandLine argv |> Parameters

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    let initialize verbose consoleMinLevel maybeSeqEndpoint =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> c.WriteTo.Console(consoleMinLevel, theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
            |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            |> fun c -> c.CreateLogger()

open System.Collections.Concurrent
open System.Diagnostics
open System.Threading

module Ingester =
    open Equinox.Cosmos.Core
    open Equinox.Cosmos.Store

    type [<NoComparison>] Span = { pos: int64; events: Equinox.Codec.IEvent<byte[]>[] }
    module Span =
        let private (|Max|) x = x.pos + x.events.LongLength
        let private trim min (Max m as x) =
            // Full remove
            if m <= min then { pos = min; events = [||] }
            // Trim until min
            elif m > min && x.pos < min then { pos = min; events = x.events |> Array.skip (min - x.pos |> int) }
            // Leave it
            else x
        let merge min (xs : Span seq) =
            let buffer = ResizeArray()
            let mutable curr = { pos = min; events = [||]}
            for x in xs |> Seq.sortBy (fun x -> x.pos) do
                match curr, trim min x with
                // no data incoming, skip
                | _, x when x.events.Length = 0 ->
                    ()
                // Not overlapping, no data buffered -> buffer
                | c, x when c.events.Length = 0 ->
                    curr <- x
                // Overlapping, join
                | Max cMax as c, x when cMax >= x.pos ->
                    curr <- { c with events = Array.append c.events (trim cMax x).events }
                // Not overlapping, new data
                | c, x ->
                    buffer.Add c
                    curr <- x
            if curr.events.Length <> 0 then buffer.Add curr
            if buffer.Count = 0 then null else buffer.ToArray()

    type [<NoComparison>] Batch = { stream: string; span: Span }
    type [<NoComparison>] Result =
        | Ok of stream: string * updatedPos: int64
        | Duplicate of stream: string * updatedPos: int64
        | Conflict of overage: Batch
        | Exn of exn: exn * batch: Batch with
        member __.WriteTo(log: ILogger) =
            match __ with
            | Ok (stream, pos) ->           log.Information("Wrote     {stream} up to {pos}", stream, pos)
            | Duplicate (stream, pos) ->    log.Information("Ignored   {stream} (synced up to {pos})", stream, pos)
            | Conflict overage ->           log.Information("Requeing  {stream} {pos} ({count} events)", overage.stream, overage.span.pos, overage.span.events.Length)
            | Exn (exn, batch) ->           log.Warning(exn,"Writing   {stream} failed, retrying {count} events ....", batch.stream, batch.span.events.Length)
    let private write (ctx : CosmosContext) ({ stream = s; span={ pos = p; events = e}} as batch) = async {
        let stream = ctx.CreateStream s
        Log.Information("Writing {s}@{i}x{n}",s,p,e.Length)
        try let! res = ctx.Sync(stream, { index = p; etag = None }, e)
            match res with
            | AppendResult.Ok _pos -> return Ok (s, p) 
            | AppendResult.Conflict (pos, _) | AppendResult.ConflictUnknown pos ->
                match pos.index, p + e.LongLength with
                | actual, expectedMax when actual >= expectedMax -> return Duplicate (s, pos.index)
                | actual, _ when p >= actual -> return Conflict batch
                | actual, _ ->
                    Log.Debug("pos {pos} batch.pos {bpos} len {blen} skip {skio}", actual, p, e.LongLength, actual-p)
                    return Conflict { stream = s; span = { pos = actual; events = e |> Array.skip (actual-p |> int) } }
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

    let inline arrayBytes (x:byte[]) = if x = null then 0 else x.Length

    type [<NoComparison>] StreamState = { read: int64 option; write: int64 option; isMalformed : bool; queue: Span[] } with
        /// Determines whether the head is ready to write (either write position is unknown, or matches)
        member __.IsHeady = Array.tryHead __.queue |> Option.exists (fun x -> __.write |> Option.forall (fun w -> w = x.pos))
        member __.IsReady = __.queue <> null && not __.isMalformed && __.IsHeady
        member __.Size =
            if __.queue = null then 0
            else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy (fun x -> arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length*2 + 16)

    let inline optionCombine f (r1: int64 option) (r2: int64 option) =
        match r1, r2 with
        | Some x, Some y -> f x y |> Some
        | None, None -> None
        | None, x | x, None -> x

    let cosmosPayloadLimit = 2 * 1024 * 1024 - 1024
    let inline cosmosPayloadBytes (x: Equinox.Codec.IEvent<byte[]>) = arrayBytes x.Data + arrayBytes x.Meta + 4
    let inline esRecPayloadBytes (x: EventStore.ClientAPI.RecordedEvent) = arrayBytes x.Data + arrayBytes x.Metadata
    let inline esPayloadBytes (x: EventStore.ClientAPI.ResolvedEvent) = esRecPayloadBytes x.Event + x.OriginalStreamId.Length * 2
    let category (s : string) = s.Split([|'-'|], 2, StringSplitOptions.RemoveEmptyEntries) |> Array.head
    let isMalformedException (e: #exn) =
        e.ToString().Contains "SyntaxError: JSON.parse Error: Unexpected input at position"
        || e.ToString().Contains "SyntaxError: JSON.parse Error: Invalid character at position"

    let combine (s1: StreamState) (s2: StreamState) : StreamState =
        let writePos = optionCombine max s1.write s2.write
        let items = seq { if s1.queue <> null then yield! s1.queue; if s2.queue <> null then yield! s2.queue }
        { read = optionCombine max s1.read s2.read; write = writePos; isMalformed = s1.isMalformed || s2.isMalformed; queue = Span.merge (defaultArg writePos 0L) items}

    /// Gathers stats relating to how many items of a given category have been observed
    type CatStats() =
        let cats = System.Collections.Generic.Dictionary<string,int>()
        member __.Ingest  cat = 
            match cats.TryGetValue cat with
            | true, catCount -> cats.[cat] <- catCount + 1
            | false, _ -> cats.[cat] <- 1
        member __.Any = cats.Count <> 0
        member __.Clear() = cats.Clear()
        member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd

    type StreamStates() =
        let states = System.Collections.Generic.Dictionary<string, StreamState>()
        let dirty = System.Collections.Generic.Queue()
        let markDirty stream = if dirty.Contains stream |> not then dirty.Enqueue stream
        
        let update stream (state : StreamState) =
            Log.Debug("Updated {s} r{r} w{w}", stream, state.read, state.write)
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
                markDirty stream |> ignore
            | true, current ->
                let updated = combine current state
                states.[stream] <- updated
                if updated.IsReady then markDirty stream |> ignore
        let updateWritePos stream pos isMalformed span =
            update stream { read = None; write = Some pos; isMalformed = isMalformed; queue = span }

        member __.Add (item: Batch, ?isMalformed) = updateWritePos item.stream 0L (defaultArg isMalformed false) [|item.span|]
        member __.HandleWriteResult = function
            | Ok (stream, pos) -> updateWritePos stream pos false null; None
            | Duplicate (stream, pos) -> updateWritePos stream pos false null; None
            | Conflict overage -> updateWritePos overage.stream overage.span.pos false [|overage.span|]; None
            | Exn (exn, batch) ->
                let malformed = isMalformedException exn
                __.Add(batch,malformed)
                if malformed then Some (category batch.stream) else None
        member __.TryPending() =
#if NET461
            if dirty.Count = 0 then None else
                let stream = dirty.Dequeue()
#else
            match dirty.TryDequeue() with
            | false, _ -> None
            | true, stream ->
#endif        
                let state = states.[stream]
                
                if not state.IsReady then None else
                
                let x = state.queue |> Array.head
                
                let mutable bytesBudget = cosmosPayloadLimit
                let mutable count = 0 
                let max2MbMax1000EventsMax10EventsFirstTranche (y : Equinox.Codec.IEvent<byte[]>) =
                    bytesBudget <- bytesBudget - cosmosPayloadBytes y
                    count <- count + 1
                    // Reduce the item count when we don't yet know the write position
                    count < (if Option.isNone state.write then 10 else 1000) && (bytesBudget >= 0 || count = 1)
                Some { stream = stream; span = { pos = x.pos; events = x.events |> Array.takeWhile max2MbMax1000EventsMax10EventsFirstTranche } }
        member __.Dump() =
            let mutable synced, ready, waiting, malformed = 0, 0, 0, 0
            let mutable readyB, waitingB, malformedB = 0L, 0L, 0L
            let waitCats = CatStats()
            for KeyValue (stream,state) in states do
                match int64 state.Size with
                | 0L -> synced <- synced + 1
                | sz when state.isMalformed -> malformed <- malformed + 1; malformedB <- malformedB + sz
                | sz when state.IsReady -> ready <- ready + 1; readyB <- readyB + sz
                | sz -> waitCats.Ingest(category stream); waiting <- waiting + 1; waitingB <- waitingB + sz
            let mb x = x / 1024L / 1024L
            Log.Warning("Syncing {dirty} Ready {ready}/{readyMb}MB Waiting {waiting}/{waitingMb}MB Malformed {malformed}/{malformedMb}MB Synced {synced}",
                dirty.Count, ready, mb readyB, waiting, mb waitingB, malformed, mb malformedB, synced)
            if waitCats.Any then Log.Warning("Waiting {waitCats}", waitCats.StatsDescending)

    type Queue(log : Serilog.ILogger, writer : Writer, cancellationToken: CancellationToken, readerQueueLen, ?interval) =
        let intervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
        let states = StreamStates()
        let results = ConcurrentQueue<_>()
        let work = new BlockingCollection<_>(ConcurrentQueue<_>(), readerQueueLen)

        member __.Add item = work.Add item
        member __.HandleWriteResult = results.Enqueue
        member __.Pump() =
            let fiveMs = TimeSpan.FromMilliseconds 5.
            let mutable pendingWriterAdd = None
            let mutable bytesPended = 0L
            let resultsHandled, ingestionsHandled, workPended, eventsPended = ref 0, ref 0, ref 0, ref 0
            let badCats = CatStats()
            let progressTimer = Stopwatch.StartNew()
            while not cancellationToken.IsCancellationRequested do
                let mutable moreResults = true
                while moreResults do
                    match results.TryDequeue() with
                    | true, res ->
                        incr resultsHandled
                        match states.HandleWriteResult res with
                        | None -> res.WriteTo log
                        | Some cat -> badCats.Ingest cat
                    | false, _ -> moreResults <- false
                let mutable t = Unchecked.defaultof<_>
                let mutable toIngest = 4096 * 5
                while work.TryTake(&t,fiveMs) && toIngest > 0 do
                    incr ingestionsHandled
                    toIngest <- toIngest - 1
                    states.Add t
                let mutable moreWork = true
                while moreWork do
                    let wrk =
                        match pendingWriterAdd with
                        | Some w ->
                            pendingWriterAdd <- None
                            Some w
                        | None ->
                            let pending = states.TryPending()
                            match pending with
                            | Some p -> Some p
                            | None ->
                                moreWork <- false
                                None
                    match wrk with
                    | None -> ()
                    | Some w ->
                        if not (writer.TryAdd(w,fiveMs)) then
                            moreWork <- false
                            pendingWriterAdd <- Some w
                        else
                            incr workPended
                            eventsPended := !eventsPended + w.span.events.Length
                            bytesPended <- bytesPended + int64 (Array.sumBy cosmosPayloadBytes w.span.events)

                if progressTimer.ElapsedMilliseconds > intervalMs then
                    progressTimer.Restart()
                    Log.Warning("Ingested {ingestions}; Sent {queued} req {events} events; Completed {completed} reqs; Egress {gb:n3}GB",
                        !ingestionsHandled, !workPended, !eventsPended,!resultsHandled, float bytesPended / 1024. / 1024. / 1024.)
                    if badCats.Any then Log.Error("Malformed {badCats}", badCats.StatsDescending); badCats.Clear()
                    ingestionsHandled := 0; workPended := 0; eventsPended := 0; resultsHandled := 0
                    states.Dump()

    /// Manages establishing of the writer 'threads' - can be Stop()ped explicitly and/or will stop when caller does
    let start(ctx : CosmosContext, writerQueueLen, writerCount, readerQueueLen) = async {
        let! ct = Async.CancellationToken
        let writer = Writer(ctx, writerQueueLen, ct)
        let queue = Queue(Log.Logger, writer, ct, readerQueueLen)
        let _ = writer.Result.Subscribe queue.HandleWriteResult // codependent, wont worry about unsubcribing
        writer.StartConsumers writerCount
        let! _ = Async.StartChild(async { queue.Pump() })
        return queue
    }

open EventStore.ClientAPI

module Reader =

    let loadSpecificStreams (conn:IEventStoreConnection, batchSize) streams (postBatch : (Ingester.Batch -> unit)) =
        let fetchStream stream =
            let rec fetchFrom pos = async {
                let! currentSlice = conn.ReadStreamEventsBackwardAsync(stream, pos, batchSize, resolveLinkTos=true) |> Async.AwaitTaskCorrect
                if currentSlice.IsEndOfStream then return () else
                let events =
                    [| for x in currentSlice.Events ->
                        let e = x.Event
                        Equinox.Codec.Core.EventData.Create (e.EventType, e.Data, e.Metadata) :> Equinox.Codec.IEvent<byte[]> |]
                postBatch { stream = stream; span = { pos = currentSlice.FromEventNumber; events = events } }
                return! fetchFrom currentSlice.NextEventNumber }
            fetchFrom 0L
        async {
            for stream in streams do
                do! fetchStream stream }

open Equinox.EventStore
open Equinox.Cosmos

let enumEvents (slice : AllEventsSlice) = seq {
    for e in slice.Events ->
        let eb = Ingester.esPayloadBytes e
        match e.Event with
        | e when not e.IsJson
            || e.EventType.StartsWith("compacted",StringComparison.OrdinalIgnoreCase)
            || e.EventStreamId.StartsWith("$") 
            || e.EventStreamId.EndsWith("_checkpoints")
            || e.EventStreamId.EndsWith("_checkpoint")
            || e.EventStreamId = "thor_useast2_to_backup_qa2_main" ->
                Choice2Of2 e
        | e when eb > Ingester.cosmosPayloadLimit ->
            Log.Error("ES Event Id {eventId} size {eventSize} exceeds Cosmos ingestion limit {maxCosmosBytes}", e.EventId, eb, Ingester.cosmosPayloadLimit)
            Choice2Of2 e
        | e -> Choice1Of2 (e.EventStreamId, e.EventNumber, Equinox.Codec.Core.EventData.Create(e.EventType, e.Data, e.Metadata))
}

type SliceStatsBuffer(?interval) =
    let intervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
    let recentCats, accStart = System.Collections.Generic.Dictionary<string,int*int>(), Stopwatch.StartNew()
    member __.Ingest(slice: AllEventsSlice) =
        let mutable batchBytes = 0
        for x in slice.Events do
            let cat = Ingester.category x.OriginalStreamId
            let eventBytes = Ingester.esPayloadBytes x
            match recentCats.TryGetValue cat with
            | true, (currCount, currSize) -> recentCats.[cat] <- (currCount + 1, currSize+eventBytes)
            | false, _ -> recentCats.[cat] <- (1, eventBytes)
            batchBytes <- batchBytes + eventBytes
        slice.Events.Length, batchBytes
    member __.DumpIfIntervalExpired() =
        if accStart.ElapsedMilliseconds > intervalMs then
            let log = function
                | [||] -> ()
                | xs ->
                    xs
                    |> Seq.sortByDescending (fun (KeyValue (_,(_,b))) -> b)
                    |> Seq.truncate 10
                    |> Seq.map (fun (KeyValue (s,(c,b))) -> b/1024/1024, s, c)
                    |> fun rendered -> Log.Warning("Processed {@cats} (MB/cat/count)", rendered)
            recentCats |> Seq.where (fun x -> x.Key.StartsWith "$" |> not) |> Array.ofSeq |> log
            recentCats |> Seq.where (fun x -> x.Key.StartsWith "$") |> Array.ofSeq |> log
            recentCats.Clear()
            accStart.Restart()

type OverallStats() =
    let mutable totalEvents, totalBytes = 0L, 0L
    member __.Ingest(batchEvents, batchBytes) = 
        totalEvents <- totalEvents + int64 batchEvents
        totalBytes <- totalBytes + int64 batchBytes
    member __.Bytes = totalBytes
    member __.Events = totalEvents

let run (destination : CosmosConnection, colls) (source : GesConnection) (spec: ReaderSpec) (writerQueueLen, writerCount, readerQueueLen) = async {
    let fetchMax = async {
        let! lastItemBatch = source.ReadConnection.ReadAllEventsBackwardAsync(Position.End, 1, resolveLinkTos = false) |> Async.AwaitTaskCorrect
        let max = lastItemBatch.NextPosition.CommitPosition
        Log.Warning("EventStore Write Position @ {pos}", max)
        return max }
    let followAll (postBatch : Ingester.Batch -> unit) = async {
        let mutable currentPos =
            match spec.start with
            | Absolute p -> EventStore.ClientAPI.Position(p,0L)
            | Percentage _ -> Position.End // placeholder, will be overwritten below
            | Start -> Position.Start
        let overall, slicesStats = OverallStats(), SliceStatsBuffer()
        let run max = async {
            let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
            let rec loop () = async {
                let! currentSlice = source.ReadConnection.ReadAllEventsForwardAsync(currentPos, spec.batchSize, resolveLinkTos = false) |> Async.AwaitTaskCorrect
                sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
                let batchEvents, batchBytes = slicesStats.Ingest currentSlice in overall.Ingest(batchEvents, batchBytes)
                slicesStats.DumpIfIntervalExpired()
                let streams =
                    enumEvents currentSlice
                    |> Seq.choose (function Choice1Of2 e -> Some e | Choice2Of2 _ -> None)
                    |> Seq.groupBy (fun (streamId,_eventNumber,_eventData) -> streamId)
                    |> Seq.map (fun (streamId,xs) -> streamId, [| for _s, i, e in xs -> i, e |])
                    |> Array.ofSeq
                let usedCats = streams |> Seq.map fst |> Seq.distinct |> Seq.length

                let postSw = Stopwatch.StartNew()
                let usedEvents = ref 0
                for stream,streamEvents in streams do
                    for pos, item in streamEvents do
                        incr usedEvents
                        postBatch { stream = stream; span =  { pos = pos; events = [| item |]}}
                let currentOffset = currentSlice.NextPosition.CommitPosition
                Log.Warning("Read {count} {ft:n3}s {mb:n1}MB {gb:n3}GB Process c {categories,2} s {streams,4} e {events,4} {pt:n0}ms Pos @ {pos} {pct:p1}",
                    batchEvents, (let e = sw.Elapsed in e.TotalSeconds), float batchBytes / 1024. / 1024., float overall.Bytes / 1024. / 1024. / 1024.,
                    usedCats, streams.Length, !usedEvents, postSw.ElapsedMilliseconds,
                    currentOffset, float currentOffset/float max)
                if currentSlice.IsEndOfStream then Log.Warning("Completed {total:n0}", overall.Events)
                sw.Restart() // restart the clock as we hand off back to the Reader
                if not currentSlice.IsEndOfStream then
                    currentPos <- currentSlice.NextPosition
                    return! loop () }
            do! loop () }
        let mutable finished = false
        let mutable max = None
        while not finished do
            try if max = None then
                    let! currMax = fetchMax
                    max <- Some currMax
                    match spec.start with
                    | Percentage pct ->
                        let rawPos = float currMax * pct / 100. |> int64
                        // @scarvel8: event_global_position = 256 x 1024 x 1024 x chunk_number + chunk_header_size (128) + event_position_offset_in_chunk
                        let chunkBase = rawPos &&& 0xFFFFFFFFE0000000L  // rawPos / 256L / 1024L / 1024L * 1024L * 1024L * 256L ;)
                        Log.Warning("Effective Start Position {pos} (chunk {chunk})", chunkBase, chunkBase >>> 29)
                        currentPos <- EventStore.ClientAPI.Position(chunkBase,0L)
                    | _ -> ()
                do! run (Option.get max)
                finished <- true
            with e -> Log.Warning(e,"Ingestion error") }
    let ctx = Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.Logger)
    let! ingester = Ingester.start(ctx, writerQueueLen, writerCount, readerQueueLen)    
    let! _ = Async.StartChild (Reader.loadSpecificStreams (source.ReadConnection, spec.batchSize) spec.streams ingester.Add)
    let! _ = Async.StartChild (followAll ingester.Add)
    do! Async.AwaitKeyboardInterrupt() }
 
[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose args.ConsoleMinLevel args.MaybeSeqEndpoint
        let source = args.EventStore.Connect(Log.Logger, Log.Logger, ConnectionStrategy.ClusterSingle NodePreference.PreferSlave) |> Async.RunSynchronously
        let readerSpec = args.BuildFeedParams()
        let writerQueueLen, writerCount, readerQueueLen = 2048,64,4096*10*10
        if Threading.ThreadPool.SetMaxThreads(512,512) |> not then failwith "Could not set max threads"
        let cosmos = args.EventStore.Cosmos // wierd nesting is due to me not finding a better way to express the semantics in Argu
        let destination = cosmos.Connnect "ProjectorTemplate" |> Async.RunSynchronously
        let colls = CosmosCollections(cosmos.Database, cosmos.Collection)
        run (destination, colls) source readerSpec (writerQueueLen, writerCount, readerQueueLen) |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1