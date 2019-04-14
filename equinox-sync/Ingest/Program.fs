module IngestTemplate.Program

open Equinox.Store // Infra
open SyncTemplate
open FSharp.Control
open Serilog
open System
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading

type StartPos = Absolute of int64 | Chunk of int | Percentage of float | Start
type ReaderSpec = { start: StartPos; stripes: int; batchSize: int; minBatchSize: int }
let mb x = float x / 1024. / 1024.

module CmdParser =
    open Argu

    exception MissingArg of string
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    module Cosmos =
        open Equinox.Cosmos
        [<NoEquality; NoComparison>]
        type Parameters =
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
                    | Connection _ ->       "specify a connection string for a Cosmos account (default: envvar:EQUINOX_COSMOS_CONNECTION)."
                    | Database _ ->         "specify a database name for Cosmos account (default: envvar:EQUINOX_COSMOS_DATABASE)."
                    | Collection _ ->       "specify a collection name for Cosmos account (default: envvar:EQUINOX_COSMOS_COLLECTION)."
                    | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                    | Retries _ ->          "specify operation retries (default: 1)."
                    | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                    | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
        type Arguments(a : ParseResults<Parameters>) =
            member __.Mode =                a.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
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
                Log.Information("CosmosDb {mode} {endpointUri} Database {database} Collection {collection}.",
                    x.Mode, endpointUri, x.Database, x.Collection)
                Log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                    (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
                let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
                c.Connect(name, discovery)

    /// To establish a local node to run against:
    ///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    ///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
    module EventStore =
        open Equinox.EventStore
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine("-v")>] VerboseStore
            | [<AltCommandLine("-o")>] Timeout of float
            | [<AltCommandLine("-r")>] Retries of int
            | [<AltCommandLine("-g")>] Host of string
            | [<AltCommandLine("-x")>] Port of int
            | [<AltCommandLine("-u")>] Username of string
            | [<AltCommandLine("-p")>] Password of string
            | [<AltCommandLine("-h")>] HeartbeatTimeout of float
            | [<AltCommandLine("-m"); Unique>] MaxItems of int
            | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Cosmos.Parameters>
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
                    | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."
                    | MaxItems _ ->         "maximum item count to request. Default: 4096"
                    | Cosmos _ ->           "specify CosmosDb parameters"
        type Arguments(a : ParseResults<Parameters> ) =
            member val Cosmos =             Cosmos.Arguments(a.GetResult Cosmos)
            member __.Host =                match a.TryGetResult Host       with Some x -> x | None -> envBackstop "Host"       "EQUINOX_ES_HOST"
            member __.Port =                match a.TryGetResult Port       with Some x -> Some x | None -> Environment.GetEnvironmentVariable "EQUINOX_ES_PORT" |> Option.ofObj |> Option.map int
            member __.Discovery =           match __.Port                   with Some p -> Discovery.GossipDnsCustomPort (__.Host, p) | None -> Discovery.GossipDns __.Host 
            member __.User =                match a.TryGetResult Username   with Some x -> x | None -> envBackstop "Username"   "EQUINOX_ES_USERNAME"
            member __.Password =            match a.TryGetResult Password   with Some x -> x | None -> envBackstop "Password"   "EQUINOX_ES_PASSWORD"
            member __.Heartbeat =           a.GetResult(HeartbeatTimeout,1.5) |> TimeSpan.FromSeconds
            member __.Timeout =             a.GetResult(Timeout,20.) |> TimeSpan.FromSeconds
            member __.Retries =             a.GetResult(Retries,3)
            member __.Connect(log: ILogger, storeLog : ILogger, connectionStrategy) =
                let s (x : TimeSpan) = x.TotalSeconds
                log.Information("EventStore {host} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}", __.Host, s __.Heartbeat, s __.Timeout, __.Retries)
                let log = if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
                let tags = ["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
                GesConnector(__.User,__.Password, __.Timeout, __.Retries, log, heartbeatTimeout=__.Heartbeat, tags=tags)
                    .Establish("IngestTemplate", __.Discovery, connectionStrategy)

    [<NoEquality; NoComparison>]
    type Parameters =
        | [<AltCommandLine "-m"; Unique>] BatchSize of int
        | [<AltCommandLine "-b"; Unique>] MinBatchSize of int
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-vc"; Unique>] VerboseConsole
        | [<AltCommandLine "-S"; Unique>] LocalSeq
        | [<AltCommandLine "-p"; Unique>] Position of int64
        | [<AltCommandLine "-c"; Unique>] Chunk of int
        | [<AltCommandLine "-P"; Unique>] Percent of float
        | [<AltCommandLine "-i"; Unique>] Stripes of int
        | [<CliPrefix(CliPrefix.None); Unique; Last>] Es of ParseResults<EventStore.Parameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | BatchSize _ ->            "maximum item count to request from feed. Default: 4096"
                | MinBatchSize _ ->         "minimum item count to drop down to in reaction to read failures. Default: 512"
                | Verbose ->                "request Verbose Logging. Default: off"
                | VerboseConsole ->         "request Verbose Console Logging. Default: off"
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | Position _ ->               "EventStore $all Stream Position to commence from"
                | Chunk _ ->                "EventStore $all Chunk to commence from"
                | Percent _ ->              "EventStore $all Stream Position to commence from (as a percentage of current tail position)"
                | Stripes _ ->              "number of concurrent readers"
                | Es _ ->                   "specify EventStore parameters"
    and Arguments(args : ParseResults<Parameters>) =
        member val EventStore =             EventStore.Arguments(args.GetResult Es)
        member __.Verbose =                 args.Contains Verbose
        member __.ConsoleMinLevel =         if args.Contains VerboseConsole then Serilog.Events.LogEventLevel.Information else Serilog.Events.LogEventLevel.Warning
        member __.MaybeSeqEndpoint =        if args.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.StartingBatchSize =       args.GetResult(BatchSize,4096)
        member __.MinBatchSize =            args.GetResult(MinBatchSize,512)
        member __.Stripes =                 args.GetResult(Stripes,1)
        member x.BuildFeedParams() : ReaderSpec =
            Log.Warning("Processing in batches of [{minBatchSize}..{batchSize}] with {stripes} stripes", x.MinBatchSize, x.StartingBatchSize, x.Stripes)
            let startPos =
                match args.TryGetResult Position, args.TryGetResult Chunk, args.TryGetResult Percent with
                | Some p, _, _ ->           Log.Warning("Processing will commence at $all Position {p}", p); Absolute p
                | _, Some c, _ ->           Log.Warning("Processing will commence at $all Chunk {c}", c); StartPos.Chunk c
                | _, _, Some p ->           Log.Warning("Processing will commence at $all Percentage {pct:P}", p/100.); Percentage p 
                | None, None, None ->       Log.Warning "Processing will commence at $all Start"; Start
            { start = startPos; batchSize = x.StartingBatchSize; minBatchSize = x.MinBatchSize; stripes = x.Stripes }

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

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

type Coordinator(log : Serilog.ILogger, writers : CosmosIngester.Writers, cancellationToken: CancellationToken, readerQueueLen, ?interval) =
    let intervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 1.) in t.TotalMilliseconds |> int64
    let states = CosmosIngester.StreamStates()
    let results = ConcurrentQueue<_>()
    let work = new BlockingCollection<_>(ConcurrentQueue<_>(), readerQueueLen)

    member __.Add item = work.Add item
    member __.HandleWriteResult = results.Enqueue
    member __.Pump() =
        let _ = writers.Result.Subscribe __.HandleWriteResult // codependent, wont worry about unsubcribing
        let fiveMs = TimeSpan.FromMilliseconds 5.
        let mutable pendingWriterAdd = None
        let mutable bytesPended = 0L
        let resultsHandled, ingestionsHandled, workPended, eventsPended = ref 0, ref 0, ref 0, ref 0
        let badCats = CosmosIngester.CatStats()
        let progressTimer = Stopwatch.StartNew()
        while not cancellationToken.IsCancellationRequested do
            let mutable moreResults, rateLimited, timedOut = true, 0, 0
            while moreResults do
                match results.TryDequeue() with
                | true, res ->
                    incr resultsHandled
                    match states.HandleWriteResult res with
                    | (stream, _), CosmosIngester.Malformed -> CosmosIngester.category stream |> badCats.Ingest
                    | _, CosmosIngester.RateLimited -> rateLimited <- rateLimited + 1
                    | _, CosmosIngester.TimedOut -> timedOut <- timedOut + 1
                    | _, CosmosIngester.Ok -> res.WriteTo log
                | false, _ -> moreResults <- false
            if rateLimited <> 0 || timedOut <> 0 then Log.Warning("Failures  {rateLimited} Rate-limited, {timedOut} Timed out", rateLimited, timedOut)
            let mutable t = Unchecked.defaultof<_>
            let mutable toIngest = 4096 * 5
            while work.TryTake(&t,fiveMs) && toIngest > 0 do
                incr ingestionsHandled
                toIngest <- toIngest - 1
                states.Add t |> ignore
            let mutable moreWork = true
            while writers.HasCapacity && moreWork do
                let pending = states.TryReady(writers.IsStreamBusy)
                match pending with
                | None -> moreWork <- false
                | Some w ->
                    incr workPended
                    eventsPended := !eventsPended + w.span.events.Length
                    bytesPended <- bytesPended + int64 (Array.sumBy CosmosIngester.cosmosPayloadBytes w.span.events)

            if progressTimer.ElapsedMilliseconds > intervalMs then
                progressTimer.Restart()
                Log.Warning("Ingested {ingestions}; Sent {queued} req {events} events; Completed {completed} reqs; Egress {gb:n3}GB",
                    !ingestionsHandled, !workPended, !eventsPended,!resultsHandled, mb bytesPended / 1024.)
                if badCats.Any then Log.Error("Malformed {badCats}", badCats.StatsDescending); badCats.Clear()
                ingestionsHandled := 0; workPended := 0; eventsPended := 0; resultsHandled := 0
                states.Dump log

    /// Manages establishing of the writer 'threads' - can be Stop()ped explicitly and/or will stop when caller does
    static member Start(log, ctx, writerQueueLen, writerCount, readerQueueLen) = async {
        let! ct = Async.CancellationToken
        let writers = CosmosIngester.Writers(CosmosIngester.Writer.write log ctx, writerCount, writerQueueLen)
        let instance = Coordinator(log, writers, ct, readerQueueLen)
        let! _ = Async.StartChild <| writers.Pump()
        let! _ = Async.StartChild(async { instance.Pump() })
        return instance
    }

type EventStore.ClientAPI.RecordedEvent with
    member __.Timestamp = System.DateTimeOffset.FromUnixTimeMilliseconds(__.CreatedEpoch)

module EventStoreReader =
    open EventStore.ClientAPI

    let inline esRecPayloadBytes (x: EventStore.ClientAPI.RecordedEvent) = CosmosIngester.arrayBytes x.Data + CosmosIngester.arrayBytes x.Metadata
    let inline esPayloadBytes (x: EventStore.ClientAPI.ResolvedEvent) = esRecPayloadBytes x.Event + x.OriginalStreamId.Length * 2

    type SliceStatsBuffer(?interval) =
        let intervalMs = let t = defaultArg interval (TimeSpan.FromMinutes 5.) in t.TotalMilliseconds |> int64
        let recentCats, accStart = System.Collections.Generic.Dictionary<string,int*int>(), Stopwatch.StartNew()
        member __.Ingest(slice: AllEventsSlice) =
            lock recentCats <| fun () ->
                let mutable batchBytes = 0
                for x in slice.Events do
                    let cat = CosmosIngester.category x.OriginalStreamId
                    let eventBytes = esPayloadBytes x
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

    type [<NoComparison>] PullResult = Exn of exn: exn | Eof | EndOfTranche
    type ReaderGroup(conn : IEventStoreConnection, enumEvents, postBatch : CosmosIngester.Batch -> unit) =
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
                        postBatch { stream = stream; span =  { index = pos; events = [| item |]}}
                if not(ignoreEmptyEof = Some true && batchEvents = 0 && not currentSlice.IsEndOfStream) then // ES doesnt report EOF on the first call :(
                    Log.Warning("Read {pos,10} {pct:p1} {ft:n3}s {mb:n1}MB {count,4} {categories,3}c {streams,4}s {events,4}e Post {pt:n0}ms",
                        range.Current.CommitPosition, range.PositionAsRangePercentage, (let e = sw.Elapsed in e.TotalSeconds), mb batchBytes,
                        batchEvents, usedCats, usedStreams, usedEvents, postSw.ElapsedMilliseconds)
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
        | Tranche of range: Range * batchSize : int
    type FeedQueue(batchSize, minBatchSize, max, ?statsInterval) =
        let work = ConcurrentQueue()
        member val OverallStats = OverallStats(?statsInterval=statsInterval)
        member val SlicesStats = SliceStatsBuffer()
        member __.AddTranche(range, ?batchSizeOverride) =
            work.Enqueue <| Work.Tranche (range, defaultArg batchSizeOverride batchSize)
        member __.AddTranche(pos, nextPos, ?batchSizeOverride) =
            __.AddTranche(Range (pos, Some nextPos, max), ?batchSizeOverride=batchSizeOverride)
        member __.TryDequeue () =
            work.TryDequeue()
        member __.Process(conn, enumEvents, postBatch, work) = async {
            let adjust batchSize = if batchSize > minBatchSize then batchSize - 128 else batchSize
            match work with
            | Tranche (range, batchSize) ->
                use _ = Serilog.Context.LogContext.PushProperty("Tranche",chunk range.Current)
                Log.Warning("Commencing tranche, batch size {bs}", batchSize)
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
                    return false }

    type Reader(conn : IEventStoreConnection, spec: ReaderSpec, enumEvents, postBatch : CosmosIngester.Batch -> unit, max, ct : CancellationToken, ?statsInterval) = 
        let work = FeedQueue(spec.batchSize, spec.minBatchSize, max, ?statsInterval=statsInterval)
        let mutable remainder =
            let startPos =
                match spec.start with
                | StartPos.Start -> Position.Start
                | Absolute p -> Position(p, 0L)
                | Chunk c -> posFromChunk c
                | Percentage pct -> posFromPercentage (pct, max)
            Log.Warning("Start Position {pos} (chunk {chunk}, {pct:p1})",
                startPos.CommitPosition, chunk startPos, float startPos.CommitPosition/ float max.CommitPosition)
            if spec.start = StartPos.Start then None
            else
                let nextPos = posFromChunkAfter startPos
                work.AddTranche(startPos, nextPos)
                Some nextPos

        member __.Pump () = async {
            let maxDop = spec.stripes
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
        let eb = EventStoreReader.esPayloadBytes e
        match e.Event with
        | e when not e.IsJson
            || e.EventType.StartsWith("compacted",StringComparison.OrdinalIgnoreCase)
            || e.EventStreamId.StartsWith("$") 
            || e.EventStreamId.EndsWith("_checkpoints")
            || e.EventStreamId.EndsWith("_checkpoint") ->
                Choice2Of2 e
        | e when eb > CosmosIngester.cosmosPayloadLimit ->
            Log.Error("ES Event Id {eventId} (#{index} in {stream}, type {type}) size {eventSize} exceeds Cosmos ingestion limit {maxCosmosBytes}",
                e.EventId, e.EventNumber, e.EventStreamId, e.EventType, eb, CosmosIngester.cosmosPayloadLimit)
            Choice2Of2 e
        | e -> Choice1Of2 (e.EventStreamId, e.EventNumber, Equinox.Codec.Core.EventData.Create(e.EventType, e.Data, e.Metadata, e.Timestamp))
}

let run (ctx : Equinox.Cosmos.Core.CosmosContext) (source : GesConnection) (spec: ReaderSpec) (writerQueueLen, writerCount, readerQueueLen) = async {
    let! coodinator = Coordinator.Start(Log.Logger, ctx, writerQueueLen, writerCount, readerQueueLen)
    let! _ = EventStoreReader.start(source.ReadConnection, spec, enumEvents, coodinator.Add)
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
            let destination = cosmos.Connect "IngestTemplate" |> Async.RunSynchronously
            let colls = CosmosCollections(cosmos.Database, cosmos.Collection)
            Equinox.Cosmos.Core.CosmosContext(destination, colls, Log.Logger)
        Thread.Sleep(1000) // https://github.com/EventStore/EventStore/issues/1899
        run ctx source readerSpec (writerQueueLen, writerCount, readerQueueLen) |> Async.RunSynchronously
        0 
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | CmdParser.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1