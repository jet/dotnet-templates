module TestbedTemplate.Services

open System

module Domain =
    module Favorites =

        // NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
        module Events =

            type Favorited =                            { date: System.DateTimeOffset; skuId: SkuId }
            type Unfavorited =                          { skuId: SkuId }
            module Compaction =
                type Snapshotted =                        { net: Favorited[] }

            type Event =
                | Snapshotted                           of Compaction.Snapshotted
                | Favorited                             of Favorited
                | Unfavorited                           of Unfavorited
                interface TypeShape.UnionContract.IUnionContract
            let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
            let (|For|) (id : ClientId) = Equinox.AggregateId("Favorites", ClientId.toString id)

        module Fold =

            type State = Events.Favorited []

            type private InternalState(input: State) =
                let dict = new System.Collections.Generic.Dictionary<SkuId, Events.Favorited>()
                let favorite (e : Events.Favorited) =   dict.[e.skuId] <- e
                let favoriteAll (xs: Events.Favorited seq) = for x in xs do favorite x
                do favoriteAll input
                member __.ReplaceAllWith xs =           dict.Clear(); favoriteAll xs
                member __.Favorite(e : Events.Favorited) =  favorite e
                member __.Unfavorite id =               dict.Remove id |> ignore
                member __.AsState() =                   Seq.toArray dict.Values

            let initial : State = [||]
            let private evolve (s: InternalState) = function
                | Events.Snapshotted { net = net } ->   s.ReplaceAllWith net
                | Events.Favorited e ->                 s.Favorite e
                | Events.Unfavorited { skuId = id } ->  s.Unfavorite id
            let fold (state: State) (events: seq<Events.Event>) : State =
                let s = InternalState state
                for e in events do evolve s e
                s.AsState()
            let isOrigin = function Events.Snapshotted _ -> true | _ -> false
            let snapshot state = Events.Snapshotted { net = state }

        type Command =
            | Favorite      of date : System.DateTimeOffset * skuIds : SkuId list
            | Unfavorite    of skuId : SkuId

        let interpret command (state : Fold.State) =
            let doesntHave skuId = state |> Array.exists (fun x -> x.skuId = skuId) |> not
            match command with
            | Favorite (date = date; skuIds = skuIds) ->
                [ for skuId in Seq.distinct skuIds do
                    if doesntHave skuId then
                        yield Events.Favorited { date = date; skuId = skuId } ]
            | Unfavorite skuId ->
                if doesntHave skuId then [] else
                [ Events.Unfavorited { skuId = skuId } ]

        type Service internal (log, resolve, maxAttempts) =

            let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

            member __.Execute(clientId, command) =
                let stream = resolve clientId
                stream.Transact(interpret command)

            member x.Favorite(clientId, skus) =
                x.Execute(clientId, Command.Favorite (DateTimeOffset.Now, skus))

            member x.Unfavorite(clientId, skus) =
                x.Execute(clientId, Command.Unfavorite skus)

            member __.List clientId : Async<Events.Favorited []> =
                let stream = resolve clientId
                stream.Query id

        let create log resolve = Service(log, resolve, maxAttempts = 3)

open Microsoft.Extensions.DependencyInjection

type StreamResolver(storage) =
    member __.Resolve
        (   codec : FsCodec.IUnionEncoder<'event, byte[], _>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
//#if memoryStore || (!cosmos && !eventStore)
        | Storage.StorageConfig.Memory store ->
            Equinox.MemoryStore.Resolver(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve
//#endif
//#if eventStore
        | Storage.StorageConfig.Es (gateway, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.Resolver<'event, 'state, _>(gateway, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve
//#endif
//#if cosmos
        | Storage.StorageConfig.Cosmos (gateway, caching, unfolds, databaseId, containerId) ->
            let store = Equinox.Cosmos.Context(gateway, databaseId, containerId)
            let accessStrategy = if unfolds then Equinox.Cosmos.AccessStrategy.Snapshot snapshot else Equinox.Cosmos.AccessStrategy.Unoptimized
            Equinox.Cosmos.Resolver<'event, 'state, _>(store, codec, fold, initial, caching, accessStrategy).Resolve
//#endif

type ServiceBuilder(storageConfig, handlerLog) =
     let resolver = StreamResolver(storageConfig)

     member __.CreateFavoritesService() =
        let fold, initial = Domain.Favorites.Fold.fold, Domain.Favorites.Fold.initial
        let snapshot = Domain.Favorites.Fold.isOrigin, Domain.Favorites.Fold.snapshot
        Domain.Favorites.create handlerLog (resolver.Resolve(Domain.Favorites.Events.codec, fold, initial, snapshot))

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()