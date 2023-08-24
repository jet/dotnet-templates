namespace Shipping.Watchdog.Integration

open Shipping.Infrastructure

type CosmosConnector(connectionString, databaseId, containerId) =

    let discovery =                     connectionString |> Equinox.CosmosStore.Discovery.ConnectionString
    let timeout =                       5. |> System.TimeSpan.FromSeconds
    let retries, maxRetryWaitTime =     5, 5. |> System.TimeSpan.FromSeconds
    let connectionMode =                Microsoft.Azure.Cosmos.ConnectionMode.Gateway
    let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, connectionMode)
    let leaseContainerId =              containerId + "-aux"
    
    new (c: Shipping.Watchdog.SourceArgs.Configuration) = CosmosConnector(c.CosmosConnection, c.CosmosDatabase, c.CosmosContainer)
    new () =                            CosmosConnector(Shipping.Watchdog.SourceArgs.Configuration EnvVar.tryGet)
    
    member val DumpStats =              Equinox.CosmosStore.Core.Log.InternalMetrics.dump
    member x.Connect() =
        let context, monitored, leases = connector.ConnectWithFeed(databaseId, containerId, leaseContainerId) |> Async.RunSynchronously
        let storeCfg = Store.Config.Cosmos (context, Equinox.Cache("Tests", sizeMb = 10)) 
        storeCfg, monitored, leases
