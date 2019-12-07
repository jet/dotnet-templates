module Location.Series

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Started = { epochId : LocationEpochId }
    type Event =
        | Started of Started
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] category = "LocationSeries"
    let (|For|) id = Equinox.AggregateId(category, LocationId.toString id)

module Fold =

    type State = LocationEpochId
    let initial = LocationEpochId.parse -1
    let evolve _state = function
        | Events.Started e -> e.epochId
    let fold = Seq.fold evolve

let interpretActivateEpoch epochId (state : Fold.State) =
    [if state < epochId then yield Events.Started { epochId = epochId }]

let toActiveEpoch state =
    if state = Fold.initial then None else Some state

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

    member __.Read(locationId) : Async<LocationEpochId option> =
        let stream = resolve locationId
        stream.Query toActiveEpoch

    member __.ActivateEpoch(locationId, epochId) : Async<unit> =
        let stream = resolve locationId
        stream.Transact(interpretActivateEpoch epochId)

let create resolve maxAttempts = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let createService (context, cache, maxAttempts) =
        create (resolve (context,cache)) maxAttempts