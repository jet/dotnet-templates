module SyncTemplate.Program

open Equinox.Cosmos
open Equinox.Cosmos.Projection
open Equinox.EventStore
open Serilog
open System
open Propulsion.Cosmos
open Propulsion.EventStore

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
        | [<AltCommandLine "-z"; Unique>] FromTail
        | [<AltCommandLine "-r"; Unique>] MaxPendingBatches of int
        | [<AltCommandLine "-w"; Unique>] MaxWriters of int
        | [<AltCommandLine "-mc"; Unique>] MaxConnections of int
        | [<AltCommandLine "-ms"; Unique>] MaxSubmit of int
        | [<AltCommandLine "-md"; Unique>] MaxDocuments of int
        | [<AltCommandLine "-l"; Unique>] LagFreqM of float
        | [<AltCommandLine "-S"; Unique>] LocalSeq
        | [<AltCommandLine "-v"; Unique>] Verbose
        | [<AltCommandLine "-vc"; Unique>] ChangeFeedVerbose
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Source of ParseResults<SourceParameters>
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | ConsumerGroupName _ ->    "Projector consumer group name."
                | MaxWriters _ ->           "maximum number of concurrent writes to target permitted. Default: 512"
                | MaxConnections _ ->       "size of Sink connection pool to maintain. Default: 1"
                | MaxPendingBatches _ ->    "maximum number of batches to let processing get ahead of completion. Default: 16"
                | MaxSubmit _ ->            "maximum number of batches to submit concurrently. Default: 8"
                | FromTail _ ->             "(iff the Consumer Name is fresh) - force skip to present Position. Default: Never skip an event."
                | MaxDocuments _ ->         "maximum item count to request from feed. Default: unlimited"
                | LagFreqM _ ->             "specify frequency (in minutes) to dump lag stats. Default: off"
                | LocalSeq ->               "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
                | Verbose ->                "request Verbose Logging. Default: off"
                | ChangeFeedVerbose ->      "request Verbose Logging from ChangeFeedProcessor. Default: off"
                | Source _ ->               "CosmosDb input parameters."
    and Arguments(a : ParseResults<Parameters>) =
        member __.MaybeSeqEndpoint =    if a.Contains LocalSeq then Some "http://localhost:5341" else None
        member __.Verbose =             a.Contains Verbose
        member __.MaxWriters =          a.GetResult(MaxWriters,1024)
        member __.ConnectionPoolSize =  a.GetResult(MaxConnections,1)
        member __.MaxReadAhead =        a.GetResult(MaxPendingBatches,16)
        member __.MaxProcessing =       a.GetResult(MaxSubmit,8)
        member __.StatsInterval =       TimeSpan.FromMinutes 1.
        member __.StatesInterval =      TimeSpan.FromMinutes 5.
        member __.VerboseConsole =      a.Contains ChangeFeedVerbose
        member __.LeaseId =             a.GetResult ConsumerGroupName
        member __.MaxDocuments =        a.TryGetResult MaxDocuments
        member __.LagFrequency =        a.TryGetResult LagFreqM |> Option.map TimeSpan.FromMinutes
        member val Source : SourceArguments = SourceArguments(a.GetResult Source)
        member __.Sink : Choice<CosmosSinkArguments,EsSinkArguments> = __.Source.Sink
        member x.BuildChangeFeedParams() =
            let disco, db =
                match a.GetResult(Source).TryGetResult LeaseCollectionSource with
                | None ->     x.Source.Discovery, { database = x.Source.Database; collection = x.Source.Collection + "-aux" }
                | Some sc ->  x.Source.Discovery, { database = x.Source.Database; collection = sc }
            Log.Information("Max read backlog: {maxPending}, of which up to {maxProcessing} processing", x.MaxReadAhead, x.MaxProcessing)
            Log.Information("Processing Lease {leaseId} in Database {db} Collection {coll} with maximum document count limited to {maxDocuments}", x.LeaseId, db.database, db.collection, Option.toNullable x.MaxDocuments)
            if a.Contains FromTail then Log.Warning("(If new projector group) Skipping projection of all existing events.")
            x.LagFrequency |> Option.iter (fun s -> Log.Information("Dumping lag stats at {lagS:n0}s intervals", s.TotalSeconds)) 
            disco, db, x.LeaseId, a.Contains FromTail, x.MaxDocuments, x.LagFrequency
    and [<NoEquality; NoComparison>] SourceParameters =
        | [<AltCommandLine "-m">] SourceConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">] SourceTimeout of float
        | [<AltCommandLine "-r">] SourceRetries of int
        | [<AltCommandLine "-rt">] SourceRetriesWaitTime of int
        | [<AltCommandLine "-s">] SourceConnection of string
        | [<AltCommandLine "-d">] SourceDatabase of string
        | [<AltCommandLine "-c"; Unique(*Mandatory is not supported*)>] SourceCollection of string
        | [<AltCommandLine "-as"; Unique>] LeaseCollectionSource of string
        | [<AltCommandLine "-e">] CategoryBlacklist of string
        | [<AltCommandLine "-i">] CategoryWhitelist of string
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Es of ParseResults<EsSinkParameters>
        | [<CliPrefix(CliPrefix.None); Unique(*ExactlyOnce is not supported*); Last>] Cosmos of ParseResults<CosmosSinkParameters>
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
                | LeaseCollectionSource _ ->"specify Collection Name for Leases collection (default: `sourcecollection` + `-aux`)."
                | CategoryBlacklist _ ->    "category whitelist"
                | CategoryWhitelist _ ->    "category blacklist"
                | Es _ ->                   "EventStore Sink parameters."
                | Cosmos _ ->               "CosmosDb Sink parameters."
    and SourceArguments(a : ParseResults<SourceParameters>) =
        member val Sink =
            match a.TryGetSubCommand() with
            | Some (Cosmos cosmos) -> Choice1Of2 (CosmosSinkArguments cosmos)
            | Some (Es es) -> Choice2Of2 (EsSinkArguments es)
            | _ -> raise (InvalidArguments "Must specify one of cosmos or es for Sink")
        member __.CategoryFilterFunction : string -> bool =
            match a.GetResults CategoryBlacklist, a.GetResults CategoryWhitelist with
            | [], [] ->     Log.Information("Not filtering by category"); fun _ -> true 
            | bad, [] ->    let black = Set.ofList bad in Log.Warning("Excluding categories: {cats}", black); fun x -> not (black.Contains x)
            | [], good ->   let white = Set.ofList good in Log.Warning("Only copying categories: {cats}", white); fun x -> white.Contains x
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
    and [<NoEquality; NoComparison>] CosmosSinkParameters =
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
                | Retries _ ->              "specify operation retries (default: 0)."
                | RetriesWaitTime _ ->      "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->       "override the connection mode (default: DirectTcp)."
    and CosmosSinkArguments(a : ParseResults<CosmosSinkParameters>) =
        member __.Mode =                a.GetResult(ConnectionMode, Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Discovery =           Discovery.FromConnectionString __.Connection
        member __.Connection =          match a.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =            match a.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =          match a.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

        member __.Timeout =             a.GetResult(CosmosSinkParameters.Timeout, 5.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(CosmosSinkParameters.Retries, 0)
        member __.MaxRetryWaitTime =    a.GetResult(RetriesWaitTime, 5)

        /// Connect with the provided parameters and/or environment variables
        member x.Connect
            /// Connection/Client identifier for logging purposes
            appName connIndex : Async<CosmosConnection> =
            let (Discovery.UriAndKey (endpointUri,_masterKey)) as discovery = x.Discovery
            Log.Information("Destination CosmosDb {mode} {endpointUri} Database {database} Collection {collection}",
                x.Mode, endpointUri, x.Database, x.Collection)
            Log.Information("Destination CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                (let t = x.Timeout in t.TotalSeconds), x.Retries, x.MaxRetryWaitTime)
            let c = CosmosConnector(x.Timeout, x.Retries, x.MaxRetryWaitTime, Log.Logger, mode=x.Mode)
            c.Connect(sprintf "App=%s Conn=%d" appName connIndex, discovery)
    and [<NoEquality; NoComparison>] EsSinkParameters =
        | [<AltCommandLine("-v")>] VerboseStore
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-g")>] Host of string
        | [<AltCommandLine("-x")>] Port of int
        | [<AltCommandLine("-u")>] Username of string
        | [<AltCommandLine("-p")>] Password of string
        | [<AltCommandLine("-h")>] HeartbeatTimeout of float
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | VerboseStore ->           "Include low level Store logging."
                | Timeout _ ->              "specify operation timeout in seconds (default: 20)."
                | Retries _ ->              "specify operation retries (default: 3)."
                | Host _ ->                 "specify a DNS query, using Gossip-driven discovery against all A records returned (defaults: envvar:EQUINOX_ES_HOST, localhost)."
                | Port _ ->                 "specify a custom port (default: envvar:EQUINOX_ES_PORT, 30778)."
                | Username _ ->             "specify a username (defaults: envvar:EQUINOX_ES_USERNAME, admin)."
                | Password _ ->             "specify a password (defaults: envvar:EQUINOX_ES_PASSWORD, changeit)."
                | HeartbeatTimeout _ ->     "specify heartbeat timeout in seconds (default: 1.5)."
    and EsSinkArguments(a : ParseResults<EsSinkParameters>) =
        member __.Host =                match a.TryGetResult Host       with Some x -> x | None -> envBackstop "Host"       "EQUINOX_ES_HOST"
        member __.Port =                match a.TryGetResult Port       with Some x -> Some x | None -> Environment.GetEnvironmentVariable "EQUINOX_ES_PORT" |> Option.ofObj |> Option.map int
        member __.Discovery =           match __.Port                   with Some p -> Discovery.GossipDnsCustomPort (__.Host, p) | None -> Discovery.GossipDns __.Host 
        member __.User =                match a.TryGetResult Username   with Some x -> x | None -> envBackstop "Username"   "EQUINOX_ES_USERNAME"
        member __.Password =            match a.TryGetResult Password   with Some x -> x | None -> envBackstop "Password"   "EQUINOX_ES_PASSWORD"
        member __.Heartbeat =           a.GetResult(HeartbeatTimeout,1.5) |> TimeSpan.FromSeconds
        member __.Timeout =             a.GetResult(Timeout,20.) |> TimeSpan.FromSeconds
        member __.Retries =             a.GetResult(Retries,3)
        member __.Connect(log: ILogger, storeLog: ILogger, connectionStrategy, appName, connIndex) =
            let s (x : TimeSpan) = x.TotalSeconds
            log.Information("EventStore {host} Connection {connId} heartbeat: {heartbeat}s Timeout: {timeout}s Retries {retries}",
                __.Host, connIndex, s __.Heartbeat, s __.Timeout, __.Retries)
            let log=if storeLog.IsEnabled Serilog.Events.LogEventLevel.Debug then Logger.SerilogVerbose storeLog else Logger.SerilogNormal storeLog
            let tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string; "App", appName; "Conn", connIndex]
            GesConnector(__.User, __.Password, __.Timeout, __.Retries, log=log, heartbeatTimeout=__.Heartbeat, tags=tags)
                .Establish("SyncTemplate", __.Discovery, connectionStrategy)

    /// Parse the commandline; can throw exceptions in response to missing arguments and/or `-h`/`--help` args
    let parse argv : Arguments =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let parser = ArgumentParser.Create<Parameters>(programName = programName)
        parser.ParseCommandLine argv |> Arguments

// Illustrates how to emit direct to the Console using Serilog
// Other topographies can be achieved by using various adapters and bridges, e.g., SerilogTarget or Serilog.Sinks.NLog
module Logging =
    open Serilog.Events
    let initialize verbose verboseConsole maybeSeqEndpoint =
        Log.Logger <-
            LoggerConfiguration()
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> // LibLog writes to the global logger, so we need to control the emission if we don't want to pass loggers everywhere
                        let cfpLevel = if verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning
                        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpLevel)
            |> fun c -> let ingesterLevel = if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information
                        c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamStates<_>>.FullName, ingesterLevel)
            |> fun c -> if verbose then c.MinimumLevel.Debug() else c
            |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                        c.MinimumLevel.Override(typeof<Propulsion.Cosmos.Internal.Writer.Result>.FullName, generalLevel)
                         .MinimumLevel.Override(typeof<Propulsion.EventStore.Internal.Writer.Result>.FullName, generalLevel)
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                            a.Logger(fun l ->
                                l.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
                                 .WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink()) |> ignore) |> ignore
                            a.Logger(fun l ->
                                let isEqx = Filters.Matching.FromSource<Core.CosmosContext>().Invoke
                                let isWriterA = Filters.Matching.FromSource<Propulsion.EventStore.Internal.Writer.Result>().Invoke
                                let isWriterB = Filters.Matching.FromSource<Propulsion.Cosmos.Internal.Writer.Result>().Invoke
                                let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
                                let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
                                let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
                                let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
                                let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
                                (if verboseConsole then l else l.Filter.ByExcluding(fun x -> isEqx x || isWriterA x || isWriterB x || isCfp x))
                                    .WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                                    |> ignore) |> ignore
                        c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=Action<_> configure)
            |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            |> fun c -> c.CreateLogger()
        Log.ForContext<Propulsion.Streams.Scheduling.StreamStates<_>>(), Log.ForContext<Core.CosmosContext>()

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

    type Microsoft.Azure.Documents.Document with
        member document.Cast<'T>() =
            let tmp = new Microsoft.Azure.Documents.Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")

    /// Maps fields in an Equinox V0 Event to the interface defined by the Propulsion.Streams library
    let (|StandardCodecEvent|) (x: EventV0) = Propulsion.Streams.Internal.EventData.Create(x.t, x.d, timestamp = x.c)

    /// We assume all Documents represent Events laid out as above
    let parse (d : Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamItem<_> =
        let (StandardCodecEvent e) as x = d.Cast<EventV0>()
        { stream = x.s; index = x.i; event = e } : _

let transformV0 categorize catFilter (v0SchemaDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamItem<_> seq = seq {
    let parsed = EventV0Parser.parse v0SchemaDocument
    let streamName = (*if parsed.Stream.Contains '-' then parsed.Stream else "Prefixed-"+*)parsed.stream
    if catFilter (categorize streamName) then
        yield parsed }
//#else
let transformOrFilter categorize catFilter (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamItem<_> seq = seq {
    for { stream = s; index = i; event = e } in DocumentParser.enumEvents changeFeedDocument do
        // NB the `index` needs to be contiguous with existing events - IOW filtering needs to be at stream (and not event) level
        if catFilter (categorize s) then
            yield {
                stream = s; index = i; event = Propulsion.Streams.Internal.EventData.Create(e.EventType, e.Data, e.Meta, e.Timestamp) } }
//#endif

let start (args : CmdParser.Arguments) =
    let log,storeLog = Logging.initialize args.Verbose args.VerboseConsole args.MaybeSeqEndpoint
    let discovery, source, connectionPolicy, catFilter = args.Source.BuildConnectionDetails()
    let auxDiscovery, aux, leaseId, startFromHere, maxDocuments, lagFrequency = args.BuildChangeFeedParams()
    let categorize (streamName : string) = streamName.Split([|'-';'_'|],2).[0]
    let sink =
        match args.Sink with
        | Choice1Of2 cosmos ->
            let colls = CosmosCollections(cosmos.Database, cosmos.Collection)
            let connect connIndex = async {
                let! c = cosmos.Connect "SyncTemplate" connIndex
                let lfc = storeLog.ForContext("ConnId", connIndex)
                return Equinox.Cosmos.Core.CosmosContext(c, colls, lfc) }
            let targets = Array.init args.ConnectionPoolSize connect |> Async.Parallel |> Async.RunSynchronously
            CosmosSink.Start(log, args.MaxReadAhead, targets, args.MaxWriters, categorize, args.StatsInterval, args.StatesInterval, args.MaxProcessing)
        | Choice2Of2 es ->
            let connect connIndex = async {
                let lfc = storeLog.ForContext("ConnId", connIndex)
                let! c = es.Connect(log, lfc, ConnectionStrategy.ClusterSingle NodePreference.Master, "SyncTemplate", connIndex)
                return GesGateway(c, GesBatchingPolicy(Int32.MaxValue)) }
            let targets = Array.init args.ConnectionPoolSize (string >> connect) |> Async.Parallel |> Async.RunSynchronously
            EventStoreSink.Start(log, storeLog, args.MaxReadAhead, targets, args.MaxWriters, categorize, args.StatsInterval, args.StatesInterval, args.MaxProcessing)
#if marveleqx
    let createObserver () = CosmosSource.CreateObserver(log, sink.StartIngester, Seq.collect (transformV0 categorize catFilter))
#else
    let createObserver () = CosmosSource.CreateObserver(log, sink.StartIngester, Seq.collect (transformOrFilter categorize catFilter))
#endif
    let runPipeline =
        CosmosSource.Run(log, discovery, connectionPolicy, source,
            aux, leaseId, startFromHere, createObserver,
            ?maxDocuments=maxDocuments, ?lagReportFreq=lagFrequency, auxDiscovery=auxDiscovery)
    sink,runPipeline

[<EntryPoint>]
let main argv =
    try try let sink, runPipeline = CmdParser.parse argv |> start
            runPipeline |> Async.Start
            sink.AwaitCompletion() |> Async.RunSynchronously
            if sink.RanToCompletion then 0 else 2
        with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
            | CmdParser.InvalidArguments msg -> eprintfn "%s" msg; 1
            | e -> eprintfn "%s" e.Message; 1
    finally Log.CloseAndFlush()