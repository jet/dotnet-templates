namespace Shipping.Watchdog.Integration

open Shipping.Watchdog.Infrastructure
open System

type CosmosStoreConfiguration(?tryGet) =
    let tryGet = defaultArg tryGet EnvVar.tryGet
    let get key =
        match tryGet key with
        | Some value -> value
        | None -> failwith $"Missing Argument/Environment Variable %s{key}"
    member _.CosmosConnection =         get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =           get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =          get "EQUINOX_COSMOS_CONTAINER"

type CosmosStoreConnector(?c : CosmosStoreConfiguration) =
    let c = match c with Some c -> c | None -> CosmosStoreConfiguration()
    let discovery =                     c.CosmosConnection |> Equinox.CosmosStore.Discovery.ConnectionString
    let timeout =                       5. |> TimeSpan.FromSeconds
    let retries, maxRetryWaitTime =     5, 5. |> TimeSpan.FromSeconds
    let connectionMode =                Microsoft.Azure.Cosmos.ConnectionMode.Gateway
    let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, connectionMode)
    let containerId =                   c.CosmosContainer
    let leaseContainerId =              containerId + "-aux"
    let connectLeases () =              connector.CreateUninitialized(c.CosmosDatabase, leaseContainerId)
    member private _.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(c.CosmosDatabase, containerId)
    member _.ConnectLeases() =
        let leases : Microsoft.Azure.Cosmos.Container = connectLeases()
        // Just as ConnectStoreAndMonitored references the global Logger, so do we -> see SerilogLogFixture, _dummy
        Serilog.Log.Information("ChangeFeed Leases Database {db} Container {container}", leases.Database.Id, leases.Id)
        leases
    member x.Connect() =
        let client, monitored = x.ConnectStoreAndMonitored()
        let storeCfg =
            let context = client |> CosmosStoreContext.create
            let cache = Equinox.Cache ("Tests", sizeMb = 10)
            Shipping.Domain.Config.Store.Cosmos (context, cache)
        storeCfg, monitored
