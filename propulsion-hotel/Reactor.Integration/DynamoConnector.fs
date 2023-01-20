namespace Reactor.Integration

open Infrastructure

type DynamoConnector(connector : Equinox.DynamoStore.DynamoStoreConnector, table, indexTable) =
    
    let client =                        connector.CreateClient()
    let storeClient =                   Equinox.DynamoStore.DynamoStoreClient(client, table)
    let storeContext =                  storeClient |> DynamoStoreContext.create
    let cache =                         Equinox.Cache("Tests", sizeMb = 10)
    
    new (c : Reactor.SourceArgs.Configuration) =
        let timeout, retries =          System.TimeSpan.FromSeconds 5., 5
        let connector =                 match c.DynamoRegion with
                                        | Some systemName -> Equinox.DynamoStore.DynamoStoreConnector(systemName, timeout, retries)
                                        | None -> Equinox.DynamoStore.DynamoStoreConnector(c.DynamoServiceUrl, c.DynamoAccessKey, c.DynamoSecretKey, timeout, retries)
        DynamoConnector(connector, c.DynamoTable, c.DynamoIndexTable)
    new () =                            DynamoConnector(Reactor.SourceArgs.Configuration EnvVar.tryGet)

    member val IndexClient =            Equinox.DynamoStore.DynamoStoreClient(client, match indexTable with Some x -> x | None -> table + "-index")
    member val DumpStats =              Equinox.DynamoStore.Core.Log.InternalMetrics.dump
    member val Store =                  Domain.Config.Store.Dynamo (storeContext, cache)
    /// Uses an in-memory checkpoint service; the real app will obviously need to store real checkpoints (see SourceArgs.Dynamo.Arguments.CreateCheckpointStore)  
    member x.CreateCheckpointService(consumerGroupName) =
        let checkpointInterval =        System.TimeSpan.FromHours 1.
        let store = Equinox.MemoryStore.VolatileStore()
        Propulsion.Feed.ReaderCheckpoint.MemoryStore.create Domain.Config.log (consumerGroupName, checkpointInterval) store