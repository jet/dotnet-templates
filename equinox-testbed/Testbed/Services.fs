module TestbedTemplate.Services

open System

module Domain =
    module Favorites =

        // NB - these schemas reflect the actual storage formats and hence need to be versioned with care
        module Events =
            type Favorited =                            { date: System.DateTimeOffset; skuId: SkuId }
            type Unfavorited =                          { skuId: SkuId }
            module Compaction =
                type Compacted =                        { net: Favorited[] }

            type Event =
                | Compacted                             of Compaction.Compacted
                | Favorited                             of Favorited
                | Unfavorited                           of Unfavorited
                interface TypeShape.UnionContract.IUnionContract

        module Folds =
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
                | Events.Compacted { net = net } ->     s.ReplaceAllWith net
                | Events.Favorited e ->                 s.Favorite e
                | Events.Unfavorited { skuId = id } ->  s.Unfavorite id
            let fold (state: State) (events: seq<Events.Event>) : State =
                let s = InternalState state
                for e in events do evolve s e
                s.AsState()
            let isOrigin = function Events.Compacted _ -> true | _ -> false
            let compact state = Events.Compacted { net = state }

        type Command =
            | Favorite      of date : System.DateTimeOffset * skuIds : SkuId list
            | Unfavorite    of skuId : SkuId

        module Commands =
            let interpret command (state : Folds.State) =
                let doesntHave skuId = state |> Array.exists (fun x -> x.skuId = skuId) |> not
                match command with
                | Favorite (date = date; skuIds = skuIds) ->
                    [ for skuId in Seq.distinct skuIds do
                        if doesntHave skuId then
                            yield Events.Favorited { date = date; skuId = skuId } ]
                | Unfavorite skuId ->
                    if doesntHave skuId then [] else
                    [ Events.Unfavorited { skuId = skuId } ]

        type Service(log, resolveStream, ?maxAttempts) =
            let (|AggregateId|) (id: ClientId) = Equinox.AggregateId("Favorites", ClientId.toStringN id)
            let (|Stream|) (AggregateId id) = Equinox.Stream(log, resolveStream id, defaultArg maxAttempts 2)
            let execute (Stream stream) command : Async<unit> =
                stream.Transact(Commands.interpret command)
            let read (Stream stream) : Async<Events.Favorited []> =
                stream.Query id

            member __.Execute(clientId, command) =
                execute clientId command

            member __.Favorite(clientId, skus) =
                execute clientId (Command.Favorite(DateTimeOffset.Now, skus))

            member __.Unfavorite(clientId, skus) =
                execute clientId (Command.Unfavorite skus)

            member __.List clientId : Async<Events.Favorited []> =
                read clientId
            
open Microsoft.Extensions.DependencyInjection

let serializationSettings = Newtonsoft.Json.JsonSerializerSettings()
let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.Codec.NewtonsoftJson.Json.Create<'Union>(serializationSettings)

type StreamResolver(storage) =
    member __.Resolve
        (   codec : Equinox.Codec.IUnionEncoder<'event,byte[]>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
//#if memoryStore || (!cosmos && !eventStore)
        | Storage.StorageConfig.Memory store ->
            Equinox.MemoryStore.Resolver(store, fold, initial).Resolve
//#endif
//#if eventStore
        | Storage.StorageConfig.Es (gateway, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.Resolver<'event,'state>(gateway, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve
//#endif
//#if cosmos
        | Storage.StorageConfig.Cosmos (gateway, caching, unfolds, databaseId, collectionId) ->
            let store = Equinox.Cosmos.Context(gateway, databaseId, collectionId)
            let accessStrategy = if unfolds then Equinox.Cosmos.AccessStrategy.Snapshot snapshot |> Some else None
            Equinox.Cosmos.Resolver<'event,'state>(store, codec, fold, initial, caching, ?access = accessStrategy).Resolve
//#endif

type ServiceBuilder(storageConfig, handlerLog) =
     let resolver = StreamResolver(storageConfig)

     member __.CreateFavoritesService() =
        let codec = genCodec<Domain.Favorites.Events.Event>()
        let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial
        let snapshot = Domain.Favorites.Folds.isOrigin,Domain.Favorites.Folds.compact
        Domain.Favorites.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot))

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()