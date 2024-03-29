namespace Shipping.Watchdog.Integration

open System

type EsdbConnector(connection, credentials) =

    let requestTimeout, retries =       TimeSpan.FromSeconds 5., 5
    let tags =                          ["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
    let connector =                     Equinox.EventStoreDb.EventStoreConnector(requestTimeout, retries, tags = tags)
    let discovery =                     let connectionString = match credentials with None -> connection | Some credentials -> String.Join(";", connection, credentials)
                                        Equinox.EventStoreDb.Discovery.ConnectionString connectionString
    let connection =                    let nodePreference = EventStore.Client.NodePreference.Leader
                                        connector.Establish(nameof EsdbConnector, discovery, Equinox.EventStoreDb.ConnectionStrategy.ClusterSingle nodePreference)
    let storeContext =                  connection |> EventStoreContext.create
    let cache =                         Equinox.Cache("Tests", sizeMb = 10)
    
    new (c: Shipping.Watchdog.SourceArgs.Configuration) =
                                        EsdbConnector(c.MaybeEventStoreConnection |> Option.defaultValue "esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false",
                                                      c.MaybeEventStoreCredentials)
    new () =                            EsdbConnector(Shipping.Watchdog.SourceArgs.Configuration EnvVar.tryGet)

    member val DumpStats =              Equinox.EventStoreDb.Log.InternalMetrics.dump
    member val EventStoreClient =       connection.ReadConnection
    member val StoreContext =           storeContext
    member val StoreArgs =              (storeContext, cache)
    member val Store =                  Store.Config<Equinox.DynamoStore.Core.EncodedBody>.Esdb (storeContext, cache)
    /// Uses an in-memory checkpoint service; the real app will obviously need to store real checkpoints (see CheckpointStore.Config)  
    member x.CreateCheckpointService(consumerGroupName) =
        let checkpointInterval =        TimeSpan.FromHours 1.
        let store = Equinox.MemoryStore.VolatileStore()
        Propulsion.Feed.ReaderCheckpoint.MemoryStore.create Store.Metrics.log (consumerGroupName, checkpointInterval) store
