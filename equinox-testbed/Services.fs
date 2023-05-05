module TestbedTemplate.Services

open System
open Equinox

module Domain =
    module Favorites =

        let [<Literal>] Category = "Favorites"
        let streamId = StreamId.gen ClientId.toString

        // NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
        module Events =

            type Favorited =                            { date: DateTimeOffset; skuId: SkuId }
            type Unfavorited =                          { skuId: SkuId }
            module Compaction =
                type Snapshotted =                        { net: Favorited[] }

            type Event =
                | Snapshotted                           of Compaction.Snapshotted
                | Favorited                             of Favorited
                | Unfavorited                           of Unfavorited
                interface TypeShape.UnionContract.IUnionContract
            let codec, codecJe = Store.Codec.gen<Event>, Store.Codec.genJsonElement<Event>

        module Fold =

            type State = Events.Favorited []

            type private InternalState(input: State) =
                let dict = System.Collections.Generic.Dictionary<SkuId, Events.Favorited>()
                let favorite (e : Events.Favorited) =   dict[e.skuId] <- e
                let favoriteAll (xs: Events.Favorited seq) = for x in xs do favorite x
                do favoriteAll input
                member _.ReplaceAllWith xs =           dict.Clear(); favoriteAll xs
                member _.Favorite(e : Events.Favorited) =  favorite e
                member _.Unfavorite id =               dict.Remove id |> ignore
                member _.AsState() =                   Seq.toArray dict.Values

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
            let toSnapshot state = Events.Snapshotted { net = state }

        let private doesntHave skuId (state : Fold.State) = state |> Array.exists (fun x -> x.skuId = skuId) |> not

        let favorite date skuIds (state : Fold.State) =
            [ for skuId in Seq.distinct skuIds do
                if state |> doesntHave skuId then
                    yield Events.Favorited { date = date; skuId = skuId } ]

        let unfavorite skuId (state : Fold.State) =
            if state |> doesntHave skuId then [] else
            [ Events.Unfavorited { skuId = skuId } ]

        type Service internal (resolve : ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

            member x.Favorite(clientId, skus) =
                let decider = resolve clientId
                decider.Transact(favorite DateTimeOffset.Now skus)

            member x.Unfavorite(clientId, sku) =
                let decider = resolve clientId
                decider.Transact(unfavorite sku)

            member _.List(clientId) : Async<Events.Favorited []> =
                let decider = resolve clientId
                decider.Query id

        let create cat =
            streamId >> Store.createDecider cat Category |> Service

        module Factory =

            let snapshot = Fold.isOrigin, Fold.toSnapshot
            let private (|Category|) = function
//#if memoryStore || (!cosmos && !eventStore)
                | Store.Context.Memory store ->
                    Store.Memory.create Events.codec Fold.initial Fold.fold store
//#endif
//#if cosmos
                | Store.Context.Cosmos (context, caching, unfolds) ->
                    let accessStrategy = if unfolds then Equinox.CosmosStore.AccessStrategy.Snapshot snapshot else Equinox.CosmosStore.AccessStrategy.Unoptimized
                    Store.Cosmos.create Events.codecJe Fold.initial Fold.fold caching accessStrategy context
//#endif
//#if eventStore
                | Store.Context.Esdb (context, caching, unfolds) ->
                    let accessStrategy = if unfolds then Equinox.EventStoreDb.AccessStrategy.RollingSnapshots snapshot |> Some else None
                    Store.Esdb.create Events.codec Fold.initial Fold.fold caching accessStrategy context
//#endif
            let create (Category cat) = streamId >> Store.createDecider cat Category |> Service

open Microsoft.Extensions.DependencyInjection

let register (services : IServiceCollection, storageConfig) =
    services.AddSingleton(Domain.Favorites.Factory.create storageConfig) |> ignore
