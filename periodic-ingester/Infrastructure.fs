[<AutoOpen>]
module PeriodicIngesterTemplate.Ingester.Infrastructure

open Serilog
open System
open System.Text
open System.Threading
open System.Threading.Tasks

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Log =

    /// Allow logging to filter out emission of log messages whose information is also surfaced as metrics
    let isStoreMetrics e = Filters.Matching.WithProperty("isMetric").Invoke e

type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(connectionName, databaseId, containerId) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDb {name} {mode} {endpointUri} timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        connectionName, o.ConnectionMode, x.Endpoint, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
        Log.Information("CosmosDb {name} Database {database} Container {container}",
                        connectionName, databaseId, containerId)

    /// Connect a CosmosStoreClient, including warming up
    member x.ConnectStore(connectionName, databaseId, containerId) =
        x.LogConfiguration(connectionName, databaseId, containerId)
        Equinox.CosmosStore.CosmosStoreClient.Connect(x.CreateAndInitialize, databaseId, containerId)

module CosmosStoreContext =

    /// Create with default packing and querying policies. Search for other `module CosmosStoreContext` impls for custom variations
    let create (storeClient : Equinox.CosmosStore.CosmosStoreClient) =
        let maxEvents = 256
        Equinox.CosmosStore.CosmosStoreContext(storeClient, tipMaxEvents=maxEvents)

/// Equinox and Propulsion provide metrics as properties in log emissions
/// These helpers wire those to pass through virtual Log Sinks that expose them as Prometheus metrics.
module Sinks =

    let tags appName = ["app", appName]

    let equinoxMetricsOnly tags (l : LoggerConfiguration) =
        l.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
         .WriteTo.Sink(Equinox.CosmosStore.Prometheus.LogSink(tags))

    let equinoxAndPropulsionConsumerMetrics tags group (l : LoggerConfiguration) =
        l |> equinoxMetricsOnly tags
          |> fun l -> l.WriteTo.Sink(Propulsion.Prometheus.LogSink(tags, group))

    let equinoxAndPropulsionFeedConsumerMetrics tags source (l : LoggerConfiguration) =
        l |> equinoxAndPropulsionConsumerMetrics tags (Propulsion.Feed.SourceId.toString source)
          |> fun l -> l.WriteTo.Sink(Propulsion.Feed.Prometheus.LogSink(tags))

    let console (configuration : LoggerConfiguration) =
        let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
        configuration.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c

    [<System.Runtime.CompilerServices.Extension>]
    static member private Sinks(configuration : LoggerConfiguration, configureMetricsSinks, configureConsoleSink, ?isMetric) =
        let configure (a : Configuration.LoggerSinkConfiguration) : unit =
            a.Logger(configureMetricsSinks >> ignore) |> ignore // unconditionally feed all log events to the metrics sinks
            a.Logger(fun l -> // but filter what gets emitted to the console sink
                let l = match isMetric with None -> l | Some predicate -> l.Filter.ByExcluding(Func<Serilog.Events.LogEvent, bool> predicate)
                configureConsoleSink l |> ignore)
            |> ignore
        configuration.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=Action<_> configure)

    [<System.Runtime.CompilerServices.Extension>]
    static member Sinks(configuration : LoggerConfiguration, configureMetricsSinks, verboseStore) =
        configuration.Sinks(configureMetricsSinks, Sinks.console, ?isMetric = if verboseStore then None else Some Log.isStoreMetrics)

type Async with
    static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)
    /// Re-raise an exception so that the current stacktrace is preserved
    static member Raise(e : #exn) : Async<'T> = Async.FromContinuations (fun (_,ec,_) -> ec e)

type StringBuilder with
    member sb.Appendf fmt = Printf.ksprintf (ignore << sb.Append) fmt
    member sb.Appendfn fmt = Printf.ksprintf (ignore << sb.AppendLine) fmt

    static member inline Build(builder : StringBuilder -> unit) =
        let instance = StringBuilder() // TOCONSIDER PooledStringBuilder.GetInstance()
        builder instance
        instance.ToString()

open System.Net
open System.Net.Http
open System.Runtime.Serialization

/// Operations on System.Net.HttpRequestMessage
module HttpReq =

    /// Creates an HTTP GET request.
    let inline create () = new HttpRequestMessage()

    /// Assigns a method to an HTTP request.
    let inline withMethod (m : HttpMethod) (req : HttpRequestMessage) =
        req.Method <- m
        req

    /// Creates an HTTP GET request.
    let inline get () = create ()

    /// Creates an HTTP POST request.
    let inline post () = create () |> withMethod HttpMethod.Post

    /// Assigns a path to an HTTP request.
    let inline withUri (u : Uri) (req : HttpRequestMessage) =
        req.RequestUri <- u
        req

    /// Assigns a path to an HTTP request.
    let inline withPath (p : string) (req : HttpRequestMessage) =
        req |> withUri (Uri(p, UriKind.Relative))

    /// Assigns a path to a Http request using printf-like formatting.
    let inline withPathf fmt =
        Printf.ksprintf withPath fmt

type HttpContent with
    member c.ReadAsString() = async {
        match c with
        | null -> return null
        | c -> return! c.ReadAsStringAsync() |> Async.AwaitTask
    }

    // only intended for logging under control of InvalidHttpResponseException, hence the esoteric name
    member internal c.ReadAsStringDiapered() = async {
        try return! c.ReadAsString()
        with :? ObjectDisposedException -> return "<HttpContent:ObjectDisposedException>"
    }

type HttpClient with
    /// <summary>
    ///     Drop-in replacement for HttpClient.SendAsync which addresses known timeout issues
    /// </summary>
    /// <param name="msg">HttpRequestMessage to be submitted.</param>
    member client.Send2(msg : HttpRequestMessage, ct : CancellationToken) = task {
        try return! client.SendAsync(msg, ct)
        // address https://github.com/dotnet/corefx/issues/20296
        with :? TaskCanceledException ->
            let message =
                match client.BaseAddress with
                | null -> "HTTP request timeout"
                | baseAddr -> sprintf "HTTP request timeout [%O]" baseAddr

            return! Async.Raise(TimeoutException message)
    }

/// Exception indicating an unexpected response received by an Http Client
type InvalidHttpResponseException =
    inherit Exception

    // TODO: include headers
    val private userMessage : string
    val private requestMethod : string
    val RequestUri : Uri
    val RequestBody : string
    val StatusCode : HttpStatusCode
    val ReasonPhrase : string
    val ResponseBody : string

    member e.RequestMethod = HttpMethod(e.requestMethod)

    private new (userMessage : string, requestMethod : HttpMethod, requestUri : Uri, requestBody : string,
                   statusCode : HttpStatusCode, reasonPhrase : string, responseBody : string,
                   ?innerException : exn) =
        {
            inherit Exception(message = null, innerException = defaultArg innerException null) ; userMessage = userMessage ;
            requestMethod = string requestMethod ; RequestUri = requestUri ; RequestBody = requestBody ;
            StatusCode = statusCode ; ReasonPhrase = reasonPhrase ; ResponseBody = responseBody
        }

    override e.Message =
        StringBuilder.Build(fun sb ->
            sb.Appendfn "%s %O RequestUri=%O HttpStatusCode=%O" e.userMessage e.RequestMethod e.RequestUri e.StatusCode
            let getBodyString str = if String.IsNullOrWhiteSpace str then "<null>" else str
            sb.Appendfn "RequestBody=%s" (getBodyString e.RequestBody)
            sb.Appendfn "ResponseBody=%s" (getBodyString e.ResponseBody))

    interface ISerializable with
        member e.GetObjectData(si : SerializationInfo, sc : StreamingContext) =
            let add name (value:obj) = si.AddValue(name, value)
            base.GetObjectData(si, sc) ; add "userMessage" e.userMessage ;
            add "requestUri" e.RequestUri ; add "requestMethod" e.requestMethod ; add "requestBody" e.RequestBody
            add "statusCode" e.StatusCode ; add "reasonPhrase" e.ReasonPhrase ; add "responseBody" e.ResponseBody

    new (si : SerializationInfo, sc : StreamingContext) =
        let get name = si.GetValue(name, typeof<'a>) :?> 'a
        {
            inherit Exception(si, sc) ; userMessage = get "userMessage" ;
            RequestUri = get "requestUri" ; requestMethod = get "requestMethod" ; RequestBody = get "requestBody" ;
            StatusCode = get "statusCode" ; ReasonPhrase = get "reasonPhrase" ; ResponseBody = get "responseBody"
        }

    static member Create(userMessage : string, response : HttpResponseMessage, ?innerException : exn) = async {
        let request = response.RequestMessage
        let! responseBodyC = response.Content.ReadAsStringDiapered() |> Async.StartChild
        let! requestBody = request.Content.ReadAsStringDiapered()
        let! responseBody = responseBodyC
        return
            InvalidHttpResponseException(
                userMessage, request.Method, request.RequestUri, requestBody,
                response.StatusCode, response.ReasonPhrase, responseBody,
                ?innerException = innerException)
    }

    static member Create(response : HttpResponseMessage, ?innerException : exn) =
        InvalidHttpResponseException.Create("HTTP request yielded unexpected response.", response, ?innerException = innerException)

type HttpResponseMessage with

    /// Raises an <c>InvalidHttpResponseException</c> if the response status code does not match expected value.
    member response.EnsureStatusCode(expectedStatusCode : HttpStatusCode) = async {
        if response.StatusCode <> expectedStatusCode then
            let! exn = InvalidHttpResponseException.Create("Http request yielded unanticipated HTTP Result.", response)
            do raise exn
    }

    /// <summary>Asynchronously deserializes the json response content using the supplied `deserializer`, without validating the `StatusCode`</summary>
    /// <param name="deserializer">The decoder routine to apply to the body content. Exceptions are wrapped in exceptions containing the offending content.</param>
    member response.InterpretContent<'Decoded>(deserializer : string -> 'Decoded) : Task<'Decoded> = task {
        let! content = response.Content.ReadAsString()
        try return deserializer content
        with e ->
            let! exn = InvalidHttpResponseException.Create("HTTP response could not be decoded.", response, e)
            return raise exn
    }

    /// <summary>Asynchronously deserializes the json response content using the supplied `deserializer`, validating the `StatusCode` is `expectedStatusCode`</summary>
    /// <param name="expectedStatusCode">check that status code matches supplied code or raise a <c>InvalidHttpResponseException</c> if it doesn't.</param>
    /// <param name="deserializer">The decoder routine to apply to the body content. Exceptions are wrapped in exceptions containing the offending content.</param>
    member response.Interpret<'Decoded>(expectedStatusCode : HttpStatusCode, deserializer : string -> 'Decoded) : Task<'Decoded> = task {
        do! response.EnsureStatusCode expectedStatusCode
        return! response.InterpretContent deserializer
    }

module HttpRes =

    let serdes = FsCodec.SystemTextJson.Options.Default |> FsCodec.SystemTextJson.Serdes
    /// Deserialize body using default Json.Net profile - throw with content details if StatusCode is unexpected or decoding fails
    let deserializeExpectedStj<'t> expectedStatusCode (res : HttpResponseMessage) =
        res.Interpret(expectedStatusCode, serdes.Deserialize<'t>)

    /// Deserialize body using default Json.Net profile - throw with content details if StatusCode is not OK or decoding fails
    let deserializeOkStj<'t> =
        deserializeExpectedStj<'t> HttpStatusCode.OK
