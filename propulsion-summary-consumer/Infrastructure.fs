[<AutoOpen>]
module ConsumerTemplate.Infrastructure

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open Serilog
open System

module Guid =

    let inline toStringN (x: Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value: ClientId): string = Guid.toStringN %value
    let parse (value: string): ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

module Streams =

    let private renderBody (x: Propulsion.Sinks.EventBody) = FsCodec.Encoding.GetStringUtf8 x
    // Uses the supplied codec to decode the supplied event record (iff at LogEventLevel.Debug, failures are logged, citing `stream` and `.Data`)
    let private tryDecode<'E> (codec: Propulsion.Sinks.Codec<'E>) (streamName: FsCodec.StreamName) event =
        match codec.Decode event with
        | ValueNone when Log.IsEnabled Serilog.Events.LogEventLevel.Debug ->
            Log.ForContext("eventData", renderBody event.Data)
                .Debug("Codec {type} Could not decode {eventType} in {stream}", codec.GetType().FullName, event.EventType, streamName)
            ValueNone
        | x -> x
    let [<return: Struct>] (|DecodeNewest|_|) codec struct (stream, events: Propulsion.Sinks.Event[]): 'E voption =
        events |> Seq.rev |> Propulsion.Internal.Seq.tryPickV (tryDecode codec stream)

    module Codec =

        let private withUpconverter<'c, 'e when 'c :> TypeShape.UnionContract.IUnionContract> up: Propulsion.Sinks.Codec<'e> =
            let down (_: 'e) = failwith "Unexpected"
            FsCodec.SystemTextJson.Codec.Create<'e, 'c, _>(up, down) |> FsCodec.Encoder.Uncompressed // options = Options.Default
        let genWithIndex<'c when 'c :> TypeShape.UnionContract.IUnionContract> : Propulsion.Sinks.Codec<int64 * 'c>  =
            let up (raw: FsCodec.ITimelineEvent<_>) e = raw.Index, e
            withUpconverter<'c, int64 * 'c> up

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role: string, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role: string, databaseId, containerId, tipMaxEvents) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents)
        c.LogConfiguration(role, databaseId, containerId)
        c

type Equinox.CosmosStore.CosmosStoreConnector with

    member private x.LogConfiguration(role, databaseId: string, containers: string[]) =
        let o = x.Options
        let timeout, retries429, timeout429 = o.RequestTimeout, o.MaxRetryAttemptsOnRateLimitedRequests, o.MaxRetryWaitTimeOnRateLimitedRequests
        Log.Information("CosmosDB {role} {mode} {endpointUri} {db} {containers} timeout {timeout}s Throttling retries {retries}, max wait {maxRetryWaitTime}s",
                        role, o.ConnectionMode, x.Endpoint, databaseId, containers, timeout.TotalSeconds, retries429, let t = timeout429.Value in t.TotalSeconds)
    member private x.Connect(role, databaseId, containers) =
        x.LogConfiguration(role, databaseId, containers)
        x.Connect(databaseId, containers)
    member x.ConnectContext(role, databaseId, containerId, tipMaxEvents) = async {
        let! client = x.Connect(role, databaseId, [| containerId |])
        return client.CreateContext(role, databaseId, containerId, tipMaxEvents) }

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration: LoggerConfiguration, ?verbose) =
        configuration
            .Enrich.FromLogContext()
        |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
        |> fun c -> let theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code
                    let t = "[{Timestamp:HH:mm:ss} {Level:u1}] {Message:lj} {Properties:j}{NewLine}{Exception}"
                    c.WriteTo.Console(theme=theme, outputTemplate=t)
