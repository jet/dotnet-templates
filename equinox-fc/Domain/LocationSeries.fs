/// Manages the active epoch for a given Location
module Fc.Location.Series

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] CategoryId = "LocationSeries"
    let (|For|) id = FsCodec.StreamName.create CategoryId (LocationId.toString id)

    type Started = { epochId : LocationEpochId }
    type Event =
        | Started of Started
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = LocationEpochId option
    let initial : State = None
    let private evolve _state = function
        | Events.Started e -> Some e.epochId
    let fold = Seq.fold evolve

let interpretAdvanceIngestionEpoch (epochId : LocationEpochId) (state : Fold.State) =
    if epochId < LocationEpochId.parse 0 then [] else

    [if state |> Option.forall (fun s -> s < epochId) then yield Events.Started { epochId = epochId }]

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For id) = Equinox.Stream<Events.Event, Fold.State>(log, resolve id, maxAttempts)

    member __.TryReadIngestionEpoch(locationId) : Async<LocationEpochId option> =
        let stream = resolve locationId
        stream.Query id

    member __.AdvanceIngestionEpoch(locationId, epochId) : Async<unit> =
        let stream = resolve locationId
        stream.Transact(interpretAdvanceIngestionEpoch epochId)

let create resolve maxAttempts = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts)

module Cosmos =

    open Equinox.Cosmos

    let resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let createService (context, cache, maxAttempts) =
        create (resolve (context, cache)) maxAttempts
