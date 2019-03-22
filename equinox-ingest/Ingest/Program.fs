module SyncTemplate.Program

open Equinox.Store // Infra
open FSharp.Control
open Serilog
open System

type StartPos = Absolute of int64 | Chunk of int | Percentage of float | Start | Ignore
type ReaderSpec = { start: StartPos; stripes: int; batchSize: int; streams: string list; tailInterval: TimeSpan option }
let mb x = float x / 1024. / 1024.

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
            member x.Connect
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
            member __.Host =     match args.TryGetResult Host       with Some x -> x | None -> envBackstop "Host"       "EQUINOX_ES_HOST"
            member __.Port =     match args.TryGetResult Port       with Some x -> Some x | None -> Environment.GetEnvironmentVariable "EQUINOX_ES_PORT" |> Option.ofObj |> Option.map int
            member __.User =     match args.TryGetResult Username   with Some x -> x | None -> envBackstop "Username"   "EQUINOX_ES_USERNAME"
            member __.Password = match args.TryGetResult Password   with Some x -> x | None -> envBackstop "Password"   "EQUINOX_ES_PASSWORD"
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
        | [<AltCommandLine "-A"; Unique>] All
        | [<AltCommandLine "-o"; Unique>] Offset of int64
        | [<AltCommandLine "-c"; Unique>] Chunk of int
        | [<AltCommandLine "-P"; Unique>] Percent of float
        | [<AltCommandLine "-i"; Unique>] Stripes of int
        | [<AltCommandLine "-t"; Unique>] Tail of intervalS: float
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Es of ParseResults<EventStore.Arguments>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | Verbose ->                "request Verbose Logging. Default: off"
                | VerboseConsole ->         "request Verbose Console Logging. Default: off"
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | Stream _ ->               "specific stream(s) to read"
                | All ->                    "traverse EventStore $all from Start"
                | Offset _ ->               "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"
                | Stripes _ ->              "number of concurrent readers"
                | Tail _ ->                 "attempt to read from tail at specified interval in Seconds"
                | Es _ ->                   "specify EventStore parameters"
    and Parameters(args : ParseResults<Arguments>) =
        member val EventStore = EventStore.Info(args.GetResult Es)
        member __.Verbose = args.Contains Verbose
        member __.ConsoleMinLevel = if args.Contains VerboseConsole then LogEventLevel.Information else LogEventLevel.Warning
        member __.MaybeSeqEndpoint = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.BatchSize = args.GetResult(BatchSize,4096)
        member __.Stripes = args.GetResult(Stripes,1)
        member __.TailInterval = match args.TryGetResult Tail with Some s -> TimeSpan.FromSeconds s |> Some | None -> None
        member x.BuildFeedParams() : ReaderSpec =
            Log.Warning("Processing in batches of {batchSize}", x.BatchSize)
            Log.Warning("Reading with {stripes} stripes", x.Stripes)
            let startPos =
                match args.TryGetResult Offset, args.TryGetResult Chunk, args.TryGetResult Percent, args.Contains All with
                | Some p, _, _, _ ->  Log.Warning("Processing will commence at $all Position {p}", p); Absolute p
                | _, Some c, _, _ ->  Log.Warning("Processing will commence at $all Chunk {c}", c); StartPos.Chunk c
                | _, _, Some p, _ ->  Log.Warning("Processing will commence at $all Percentage {pct:P}", p/100.); Percentage p 
                | None, None, None, true -> Log.Warning "Processing will commence at $all Start"; Start
                | None, None, None, false -> Log.Warning "No $all processing requested"; Ignore
            match x.TailInterval with
            | Some interval -> Log.Warning("Following tail at {seconds}s interval", interval.TotalSeconds)
            | None -> Log.Warning "Not following tail"
            { start = startPos; stripes = x.Stripes; batchSize = x.BatchSize; streams = args.GetResults Stream; tailInterval = x.TailInterval }

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
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {Tranche} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(consoleMinLevel, theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
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
            match dirty |> Queue.tryDequeue with
            | None -> None
            | Some stream ->
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
            Log.Warning("Syncing {dirty} Ready {ready}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB Synced {synced}",
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
                        !ingestionsHandled, !workPended, !eventsPended,!resultsHandled, mb bytesPended / 1024.)
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

module EventStoreReader =
    open EventStore.ClientAPI

    type SliceStatsBuffer(?interval) =
        let intervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 5.) in t.TotalMilliseconds |> int64
        let recentCats, accStart = System.Collections.Generic.Dictionary<string,int*int>(), Stopwatch.StartNew()
        member __.Ingest(slice: AllEventsSlice) =
            lock recentCats <| fun () ->
                let mutable batchBytes = 0
                for x in slice.Events do
                    let cat = Ingester.category x.OriginalStreamId
                    let eventBytes = Ingester.esPayloadBytes x
                    match recentCats.TryGetValue cat with
                    | true, (currCount, currSize) -> recentCats.[cat] <- (currCount + 1, currSize+eventBytes)
                    | false, _ -> recentCats.[cat] <- (1, eventBytes)
                    batchBytes <- batchBytes + eventBytes
                __.DumpIfIntervalExpired()
                slice.Events.Length, int64 batchBytes
        member __.DumpIfIntervalExpired(?force) =
            if accStart.ElapsedMilliseconds > intervalMs || defaultArg force false then
                lock recentCats <| fun () ->
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

    type OverallStats(?statsInterval) =
        let intervalMs = let t = defaultArg statsInterval (TimeSpan.FromMinutes 5.) in t.TotalMilliseconds |> int64
        let overallStart, progressStart = Stopwatch.StartNew(), Stopwatch.StartNew()
        let mutable totalEvents, totalBytes = 0L, 0L
        member __.Ingest(batchEvents, batchBytes) = 
            Interlocked.Add(&totalEvents,batchEvents) |> ignore
            Interlocked.Add(&totalBytes,batchBytes) |> ignore
        member __.Bytes = totalBytes
        member __.Events = totalEvents
        member __.DumpIfIntervalExpired(?force) =
            if progressStart.ElapsedMilliseconds > intervalMs || force = Some true then
                let totalMb = mb totalBytes
                Log.Warning("Traversed {events} events {gb:n1}GB {mbs:n2}MB/s", totalEvents, totalMb/1024., totalMb*1000./float overallStart.ElapsedMilliseconds)
                progressStart.Restart()

    type Range(start, sliceEnd : Position option, max : Position) =
        member val Current = start with get, set
        member __.TryNext(pos: Position) =
            __.Current <- pos
            __.IsCompleted
        member __.IsCompleted =
            match sliceEnd with
            | Some send when __.Current.CommitPosition >= send.CommitPosition -> false
            | _ -> true
        member __.PositionAsRangePercentage =
            if max.CommitPosition=0L then Double.NaN
            else float __.Current.CommitPosition/float max.CommitPosition

    // @scarvel8: event_global_position = 256 x 1024 x 1024 x chunk_number + chunk_header_size (128) + event_position_offset_in_chunk
    let chunk (pos: Position) = uint64 pos.CommitPosition >>> 28
    let posFromChunk (chunk: int) =
        let chunkBase = int64 chunk * 1024L * 1024L * 256L
        Position(chunkBase,0L)
    let posFromPercentage (pct,max : Position) =
        let rawPos = Position(float max.CommitPosition * pct / 100. |> int64, 0L)
        let chunk = int (chunk rawPos) in posFromChunk chunk // &&& 0xFFFFFFFFE0000000L // rawPos / 256L / 1024L / 1024L * 1024L * 1024L * 256L
    let posFromChunkAfter (pos: Position) =
        let nextChunk = 1 + int (chunk pos)
        posFromChunk nextChunk

    let fetchMax (conn : IEventStoreConnection) = async {
        let! lastItemBatch = conn.ReadAllEventsBackwardAsync(Position.End, 1, resolveLinkTos = false) |> Async.AwaitTaskCorrect
        let max = lastItemBatch.NextPosition
        Log.Warning("EventStore {chunks} chunks, ~{gb:n1}GB Write Position @ {pos} ", chunk max, mb max.CommitPosition/1024., max.CommitPosition)
        return max }
    let establishMax (conn : IEventStoreConnection) = async {
        let mutable max = None
        while Option.isNone max do
            try let! max_ = fetchMax conn
                max <- Some max_
            with e ->
                Log.Warning(e,"Could not establish max position")
                do! Async.Sleep 5000 
        return Option.get max }
    let pullStream (conn : IEventStoreConnection, batchSize) stream (postBatch : Ingester.Batch -> unit) =
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

    type [<NoComparison>] PullResult = Exn of exn: exn | Eof | EndOfTranche
    type ReaderGroup(conn : IEventStoreConnection, enumEvents, postBatch : Ingester.Batch -> unit) =
        member __.Pump(range : Range, batchSize, slicesStats : SliceStatsBuffer, overallStats : OverallStats, ?ignoreEmptyEof) =
            let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
            let rec loop () = async {
                let! currentSlice = conn.ReadAllEventsForwardAsync(range.Current, batchSize, resolveLinkTos = false) |> Async.AwaitTaskCorrect
                sw.Stop() // Stop the clock after ChangeFeedProcessor hands off to us
                let postSw = Stopwatch.StartNew()
                let batchEvents, batchBytes = slicesStats.Ingest currentSlice in overallStats.Ingest(int64 batchEvents, batchBytes)
                let streams =
                    enumEvents currentSlice.Events
                    |> Seq.choose (function Choice1Of2 e -> Some e | Choice2Of2 _ -> None)
                    |> Seq.groupBy (fun (streamId,_eventNumber,_eventData) -> streamId)
                    |> Seq.map (fun (streamId,xs) -> streamId, [| for _s, i, e in xs -> i, e |])
                    |> Array.ofSeq
                let usedStreams, usedCats = streams.Length, streams |> Seq.map fst |> Seq.distinct |> Seq.length
                let mutable usedEvents = 0
                for stream,streamEvents in streams do
                    for pos, item in streamEvents do
                        usedEvents <- usedEvents + 1
                        postBatch { stream = stream; span =  { pos = pos; events = [| item |]}}
                if not(ignoreEmptyEof = Some true && batchEvents = 0 && not currentSlice.IsEndOfStream) then // ES doesnt report EOF on the first call :(
                    Log.Warning("Read {pos,10} {pct:p1} {ft:n3}s {count,4} {mb:n1}MB {categories,3}c {streams,4}s {events,4}e Post {pt:n0}ms",
                        range.Current.CommitPosition, range.PositionAsRangePercentage, (let e = sw.Elapsed in e.TotalSeconds), batchEvents, mb batchBytes,
                        usedCats, usedStreams, usedEvents, postSw.ElapsedMilliseconds)
                let shouldLoop = range.TryNext currentSlice.NextPosition
                if shouldLoop && not currentSlice.IsEndOfStream then
                    sw.Restart() // restart the clock as we hand off back to the Reader
                    return! loop ()
                else
                    return currentSlice.IsEndOfStream }
            async {
                try let! eof = loop ()
                    return if eof then Eof else EndOfTranche
                with e -> return Exn e }

    type [<NoComparison>] Work =
        | Stream of name: string * batchSize: int
        | Tranche of range: Range * batchSize : int
        | Tail of pos: Position * interval: TimeSpan * batchSize : int
    type FeedQueue(batchSize, max, ?statsInterval) =
        let work = ConcurrentQueue()
        member val OverallStats = OverallStats(?statsInterval=statsInterval)
        member val SlicesStats = SliceStatsBuffer()
        member __.AddTranche(range, ?batchSizeOverride) =
            work.Enqueue <| Work.Tranche (range, defaultArg batchSizeOverride batchSize)
        member __.AddTranche(pos, nextPos, ?batchSizeOverride) =
            __.AddTranche(Range (pos, Some nextPos, max), ?batchSizeOverride=batchSizeOverride)
        member __.AddStream(name, ?batchSizeOverride) =
            work.Enqueue <| Work.Stream (name, defaultArg batchSizeOverride batchSize)
        member __.AddTail(pos, interval, ?batchSizeOverride) =
            work.Enqueue <| Work.Tail (pos, interval, defaultArg batchSizeOverride batchSize)
        member __.TryDequeue () =
            work.TryDequeue()
        member __.Process(conn, enumEvents, postBatch, work) = async {
            let adjust batchSize = if batchSize > 128 then batchSize - 128 else batchSize
            match work with
            | Stream (name,batchSize) ->
                use _ = Serilog.Context.LogContext.PushProperty("Stream",name)
                Log.Warning("Reading stream; batch size {bs}", batchSize)
                try do! pullStream (conn, batchSize) name postBatch
                with e ->
                    let bs = adjust batchSize
                    Log.Warning(e,"Could not read stream, retrying with batch size {bs}", bs)
                    __.AddStream(name, bs)
                return false
            | Tranche (range, batchSize) ->
                use _ = Serilog.Context.LogContext.PushProperty("Tranche",chunk range.Current)
                Log.Warning("Reading chunk; batch size {bs}", batchSize)
                let reader = ReaderGroup(conn, enumEvents, postBatch)
                let! res = reader.Pump(range, batchSize, __.SlicesStats, __.OverallStats)
                match res with
                | PullResult.EndOfTranche ->
                    Log.Warning("Completed tranche")
                    __.OverallStats.DumpIfIntervalExpired()
                    return false
                | PullResult.Eof ->
                    Log.Warning("REACHED THE END!")
                    __.OverallStats.DumpIfIntervalExpired(true)
                    return true
                | PullResult.Exn e ->
                    let bs = adjust batchSize
                    Log.Warning(e, "Could not read All, retrying with batch size {bs}", bs)
                    __.OverallStats.DumpIfIntervalExpired()
                    __.AddTranche(range, bs) 
                    return false 
            | Tail (pos, interval, batchSize) ->
                let mutable first, count, batchSize, range = true, 0, batchSize, Range(pos,None, Position.Start)
                let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
                let progressIntervalMs, tailIntervalMs = int64 statsInterval.TotalMilliseconds, int64 interval.TotalMilliseconds
                let progressSw, tailSw = Stopwatch.StartNew(), Stopwatch.StartNew()
                let reader = ReaderGroup(conn, enumEvents, postBatch)
                let slicesStats, stats = SliceStatsBuffer(), OverallStats()
                while true do
                    let currentPos = range.Current
                    use _ = Serilog.Context.LogContext.PushProperty("Tranche", "Tail")
                    if first then
                        first <- false
                        Log.Warning("Tailing at {interval}s interval", interval.TotalSeconds)
                    elif progressSw.ElapsedMilliseconds > progressIntervalMs then
                        Log.Warning("Performed {count} tails to date @ {pos} chunk {chunk}", count, currentPos.CommitPosition, chunk currentPos)
                        progressSw.Restart()
                    count <- count + 1
                    let! res = reader.Pump(range,batchSize,slicesStats,stats,ignoreEmptyEof=true)
                    stats.DumpIfIntervalExpired()
                    match tailIntervalMs - tailSw.ElapsedMilliseconds with
                    | waitTimeMs when waitTimeMs > 0L -> do! Async.Sleep (int waitTimeMs)
                    | _ -> ()
                    tailSw.Restart()
                    match res with
                    | PullResult.EndOfTranche | PullResult.Eof -> ()
                    | PullResult.Exn e ->
                        batchSize <- adjust batchSize
                        Log.Warning(e, "Tail $all failed, adjusting batch size to {bs}", batchSize)
                return true }

    type Reader(conn : IEventStoreConnection, spec: ReaderSpec, enumEvents, postBatch : Ingester.Batch -> unit, max, ct : CancellationToken, ?statsInterval) = 
        let work = FeedQueue(spec.batchSize, max, ?statsInterval=statsInterval)
        do  match spec.tailInterval with
            | Some interval -> work.AddTail(max, interval)
            | None -> ()
            for s in spec.streams do
                work.AddStream s
        let mutable remainder =
            let startPos =
                match spec.start with
                | StartPos.Start -> Position.Start
                | Absolute p -> Position(p, 0L)
                | Chunk c -> posFromChunk c
                | Percentage pct -> posFromPercentage (pct, max)
                | Ignore -> max
            Log.Warning("Start Position {pos} (chunk {chunk}, {pct:p1})",
                startPos.CommitPosition, chunk startPos, float startPos.CommitPosition/ float max.CommitPosition)
            if spec.start = Ignore then None
            else
                let nextPos = posFromChunkAfter startPos
                work.AddTranche(startPos, nextPos)
                Some nextPos

        member __.Pump () = async {
            (*if spec.tail then enqueue tail work*)
            let maxDop = spec.stripes + Option.count spec.tailInterval
            let dop = new SemaphoreSlim(maxDop)
            let mutable finished = false
            while not ct.IsCancellationRequested && not (finished && dop.CurrentCount <> maxDop) do
                let! _ = dop.Await()
                work.OverallStats.DumpIfIntervalExpired()
                let forkRunRelease task = async {
                    let! _ = Async.StartChild <| async {
                        try let! eof = work.Process(conn, enumEvents, postBatch, task)
                            if eof then remainder <- None
                        finally dop.Release() |> ignore }
                    return () }
                match work.TryDequeue() with
                | true, task ->
                    do! forkRunRelease task
                | false, _ ->
                    match remainder with
                    | Some pos -> 
                        let nextPos = posFromChunkAfter pos
                        remainder <- Some nextPos
                        do! forkRunRelease <| Work.Tranche (Range(pos, Some nextPos, max), spec.batchSize)
                    | None ->
                        if finished then do! Async.Sleep 1000 
                        else Log.Warning("No further ingestion work to commence")
                        finished <- true }

    let start (conn, spec, enumEvents, postBatch) = async {
        let! ct = Async.CancellationToken
        let! max = establishMax conn 
        let reader = Reader(conn, spec, enumEvents, postBatch, max, ct)
        let! _ = Async.StartChild <| reader.Pump()
        return ()
    }

open Equinox.Cosmos
open Equinox.EventStore

let enumEvents (xs : EventStore.ClientAPI.ResolvedEvent[]) = seq {
    for e in xs ->
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

let run (ctx : Equinox.Cosmos.Core.CosmosContext) (source : GesConnection) (spec: ReaderSpec) (writerQueueLen, writerCount, readerQueueLen) = async {
    let! ingester = Ingester.start(ctx, writerQueueLen, writerCount, readerQueueLen)
    let! _feeder = EventStoreReader.start(source.ReadConnection, spec, enumEvents, ingester.Add)
    do! Async.AwaitKeyboardInterrupt() }
 
[<EntryPoint>]
let main argv =
    try let args = CmdParser.parse argv
        Logging.initialize args.Verbose args.ConsoleMinLevel args.MaybeSeqEndpoint
        let source = args.EventStore.Connect(Log.Logger, Log.Logger, ConnectionStrategy.ClusterSingle NodePreference.PreferSlave) |> Async.RunSynchronously
        let readerSpec = args.BuildFeedParams()
        let writerQueueLen, writerCount, readerQueueLen = 2048,64,4096*10*10
        let cosmos = args.EventStore.Cosmos // wierd nesting is due to me not finding a better way to express the semantics in Argu
        let ctx =
            let destination = cosmos.Connect "ProjectorTemplate" |> Async.RunSynchronously
            let colls = CosmosCollections(cosmos.Database, cosmos.Collection)
            Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.Logger)
        run ctx source readerSpec (writerQueueLen, writerCount, readerQueueLen) |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1