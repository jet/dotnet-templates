namespace Shipping.Watchdog.Integration

open Shipping.Infrastructure

type DynamoConnector(serviceUrl, accessKey, secretKey, table, indexTable) =
    
    let requestTimeout, retries =       System.TimeSpan.FromSeconds 5., 5
    let connector =                     Equinox.DynamoStore.DynamoStoreConnector(serviceUrl, accessKey, secretKey, requestTimeout, retries)
    let client =                        connector.CreateClient()
    let storeClient =                   Equinox.DynamoStore.DynamoStoreClient(client, table)
    let storeContext =                  storeClient |> DynamoStoreContext.create
    let cache =                         Equinox.Cache("Tests", sizeMb = 10)
    
    new (c : Shipping.Watchdog.Program.Configuration) = DynamoConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, c.DynamoTable, c.DynamoIndexTable)
    new () =                            DynamoConnector(Shipping.Watchdog.Program.Configuration EnvVar.tryGet)

    member val DumpStats =              Equinox.DynamoStore.Core.Log.InternalMetrics.dump
    member val IndexClient =            Equinox.DynamoStore.DynamoStoreClient(client, match indexTable with Some x -> x | None -> table + "-index")
    member val StoreContext =           storeContext
    member val StoreArgs =              (storeContext, cache)
    member val Store =                  Shipping.Domain.Config.Store<Equinox.DynamoStore.Core.EncodedBody>.Dynamo (storeContext, cache)
    member x.CreateCheckpointService(consumerGroupName) = x.IndexClient.CreateCheckpointService(consumerGroupName, cache)
