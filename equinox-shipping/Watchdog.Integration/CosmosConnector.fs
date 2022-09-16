namespace Shipping.Watchdog.Integration

open Shipping.Infrastructure

type CosmosConnector(connectionString, databaseId, containerId) =

    let discovery =                     connectionString |> Equinox.CosmosStore.Discovery.ConnectionString
    let timeout =                       5. |> System.TimeSpan.FromSeconds
    let retries, maxRetryWaitTime =     5, 5. |> System.TimeSpan.FromSeconds
    let connectionMode =                Microsoft.Azure.Cosmos.ConnectionMode.Gateway
    let connector =                     Equinox.CosmosStore.CosmosStoreConnector(discovery, timeout, retries, maxRetryWaitTime, connectionMode)
    let leaseContainerId =              containerId + "-aux"
    let connectLeases () =              connector.CreateUninitialized(databaseId, leaseContainerId)
    
    new (c : Shipping.Watchdog.SourceArgs.Configuration) = CosmosConnector(c.CosmosConnection, c.CosmosDatabase, c.CosmosContainer)
    new () =                            CosmosConnector(Shipping.Watchdog.SourceArgs.Configuration EnvVar.tryGet)
    
    member val DumpStats =              Equinox.CosmosStore.Core.Log.InternalMetrics.dump
    member private _.ConnectStoreAndMonitored() = connector.ConnectStoreAndMonitored(databaseId, containerId)
    member _.ConnectLeases() =
        let leases : Microsoft.Azure.Cosmos.Container = connectLeases()
        // Just as ConnectStoreAndMonitored references the global Logger, so do we -> see SerilogLogFixture, _dummy
        Serilog.Log.Information("ChangeFeed Leases Database {db} Container {container}", leases.Database.Id, leases.Id)
        leases
    member x.Connect() =
        let client, monitored = x.ConnectStoreAndMonitored()
        let storeCfg =
            let context = client |> CosmosStoreContext.create
            let cache = Equinox.Cache("Tests", sizeMb = 10)
            Shipping.Domain.Config.Store.Cosmos (context, cache)
        storeCfg, monitored
