namespace Shipping.Watchdog.Integration

open Equinox.DynamoStore
open Shipping.Watchdog.Infrastructure

type DynamoConnector(serviceUrl, accessKey, secretKey, table, indexTable) =
    
    let requestTimeout, retries =               System.TimeSpan.FromSeconds 5., 5
    let connector =                             DynamoStoreConnector(serviceUrl, accessKey, secretKey, requestTimeout, retries)
    let client =                                connector.CreateClient()
    let storeClient =                           DynamoStoreClient(client, table)
    let storeContext =                          storeClient |> DynamoStoreContext.create
    let cache =                                 Equinox.Cache("Tests", sizeMb = 10)
    
    new (c : Shipping.Watchdog.Program.Configuration) = DynamoConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, c.DynamoTable, c.DynamoIndexTable)
    new () = DynamoConnector(Shipping.Watchdog.Program.Configuration EnvVar.tryGet)

    member val IndexClient =                    DynamoStoreClient(client, match indexTable with Some x -> x | None -> table + "-index")
    member val StoreContext =                   storeContext
    member _.DynamoStore =                      (storeContext, cache)
    member _.Store =                            Shipping.Domain.Config.Store<Core.EncodedBody>.Dynamo (storeContext, cache)
