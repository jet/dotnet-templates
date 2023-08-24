[<AutoOpen>]
module TestbedTemplate.Infrastructure

open FSharp.UMX
open Serilog
open System

module Guid =
    let inline toStringN (x: Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId = let toString (value: ClientId): string = Guid.toStringN %value

/// SkuId strongly typed id; represented internally as a Guid
// NB Perf is suboptimal as a key, see Equinox's samples/Store for expanded version
type SkuId = Guid<skuId>
and [<Measure>] skuId
module SkuId = let toString (value: SkuId): string = Guid.toStringN %value

module Store =

    let log = Serilog.Log.ForContext("isMetric", true)

module EnvVar =

    let tryGet varName: string option = Environment.GetEnvironmentVariable varName |> Option.ofObj

type Equinox.CosmosStore.CosmosStoreContext with

    member x.LogConfiguration(role: string, databaseId: string, containerId: string) =
        Log.Information("CosmosStore {role:l} {db}/{container} Tip maxEvents {maxEvents} maxSize {maxJsonLen} Query maxItems {queryMaxItems}",
                        role, databaseId, containerId, x.TipOptions.MaxEvents, x.TipOptions.MaxJsonLength, x.QueryOptions.MaxItems)

type Equinox.CosmosStore.CosmosStoreClient with

    member x.CreateContext(role, databaseId, containerId, tipMaxEvents, queryMaxItems) =
        let c = Equinox.CosmosStore.CosmosStoreContext(x, databaseId, containerId, tipMaxEvents, queryMaxItems = queryMaxItems)
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
    member x.ConnectContext(role, databaseId, containerId, tipMaxEvents, queryMaxItems) = async {
        let! client = x.Connect(role, databaseId, [| containerId |])
        return client.CreateContext(role, databaseId, containerId, tipMaxEvents, queryMaxItems = queryMaxItems) }

type Exception with
    // https://github.com/fsharp/fslang-suggestions/issues/660
    member this.Reraise () =
        (System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture this).Throw ()
        Unchecked.defaultof<_>
