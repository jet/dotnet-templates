namespace Reactor.Integration

type MessageDbConnector(connectionString: string) =
    
    let client =                        Equinox.MessageDb.MessageDbClient connectionString
    let context =                       Equinox.MessageDb.MessageDbContext client
    let cache =                         Equinox.Cache("Tests", sizeMb = 10)
    
    new (c: Reactor.SourceArgs.Configuration) = MessageDbConnector(c.MdbConnectionString)
    new () =                            MessageDbConnector(Reactor.SourceArgs.Configuration EnvVar.tryGet)

    member val ConnectionString =       connectionString
    member val DumpStats =              Equinox.MessageDb.Log.InternalMetrics.dump
    member val Store =                  Domain.Store.Config.Mdb (context, cache)
    /// Uses an in-memory checkpoint service; the real app will obviously need to store real checkpoints (see SourceArgs.Mdb.Arguments.CreateCheckpointStore)  
    member x.CreateCheckpointService(consumerGroupName) =
        let checkpointInterval =        System.TimeSpan.FromHours 1.
        let store = Equinox.MemoryStore.VolatileStore()
        Propulsion.Feed.ReaderCheckpoint.MemoryStore.create Domain.Store.Metrics.log (consumerGroupName, checkpointInterval) store
