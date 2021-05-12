[<AutoOpen>]
module FeedConsumerTemplate.HttpHelpers

open Serilog
open System
open System.Text

module EnvVar =

    let tryGet varName : string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                    c.WriteTo.Console(theme=theme, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {NewLine}{Exception}")

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
    member client.Send(msg : HttpRequestMessage) = async {
        let! ct = Async.CancellationToken
        try return! client.SendAsync(msg, ct) |> Async.AwaitTask
        // address https://github.com/dotnet/corefx/issues/20296
        with :? System.Threading.Tasks.TaskCanceledException ->
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
            new InvalidHttpResponseException(
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
    member response.InterpretContent<'Decoded>(deserializer : string -> 'Decoded) : Async<'Decoded> = async {
        let! content = response.Content.ReadAsString()
        try return deserializer content
        with e ->
            let! exn = InvalidHttpResponseException.Create("HTTP response could not be decoded.", response, e)
            return raise exn
    }

    /// <summary>Asynchronously deserializes the json response content using the supplied `deserializer`, validating the `StatusCode` is `expectedStatusCode`</summary>
    /// <param name="expectedStatusCode">check that status code matches supplied code or raise a <c>InvalidHttpResponseException</c> if it doesn't.</param>
    /// <param name="deserializer">The decoder routine to apply to the body content. Exceptions are wrapped in exceptions containing the offending content.</param>
    member response.Interpret<'Decoded>(expectedStatusCode : HttpStatusCode, deserializer : string -> 'Decoded) : Async<'Decoded> = async {
        do! response.EnsureStatusCode expectedStatusCode
        return! response.InterpretContent deserializer
    }

module HttpRes =

    /// Deserialize body using default Json.Net profile - throw with content details if StatusCode is unexpected or decoding fails
    let deserializeExpectedJsonNet<'t> expectedStatusCode (res : HttpResponseMessage) =
        res.Interpret(expectedStatusCode, Newtonsoft.Json.JsonConvert.DeserializeObject<'t>)

    /// Deserialize body using default Json.Net profile - throw with content details if StatusCode is not OK or decoding fails
    let deserializeOkJsonNet<'t> =
        deserializeExpectedJsonNet<'t> HttpStatusCode.OK
