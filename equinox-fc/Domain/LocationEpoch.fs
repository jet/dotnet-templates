/// Manages Stock adjustments and deltas for a given Location
/// Provides for controlled opening and closing of an epoch, carrying forward incoming balances when a given Epoch reaches a 'full' state
/// See Location.Service for the logic that allows competing readers/writers to co-operate in bringing this about
module Fc.Location.Epoch

let [<Literal>] Category = "LocationEpoch"
let streamName locationId = FsCodec.StreamName.create Category (LocationId.toString locationId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Event =
        | Added          of {| delta : int; id : InventoryTransactionId |}
        | Removed        of {| delta : int; id : InventoryTransactionId |}
        | Reset          of {| value : int; id : InventoryTransactionId |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = Record list    // reverse order, i.e. most recent first
    and Record = { balance : Balance; id : InventoryTransactionId }
    and Balance = int
    let (|Current|) = function [] -> 0 | { balance = bal } :: _ -> bal
    let initial = []
    let evolve state event =
        match event, state with
        | Events.Added e,   (Current cur as log) -> { id = e.id ; balance = cur + e.delta } :: log
        | Events.Removed e, (Current cur as log) -> { id = e.id ; balance = cur - e.delta } :: log
        | Events.Reset e,   log                  -> { id = e.id ; balance = e.value       } :: log
    let fold : State -> Events.Event seq -> State= Seq.fold evolve

type Command =
    | Reset  of value : int
    | Add    of delta : int
    | Remove of delta : int

type Result = Denied | Accepted of Fold.Balance

let decide transactionId command (state: Fold.State) =
    match state |> Seq.tryPick (function { balance = bal; id = id } when id = transactionId -> Some bal | _ -> None) with
    | Some bal ->
        Accepted bal, []
    | None ->
        let events =
            let (Fold.Current openingBal) = state
            match command with
            | Reset  value -> [Events.Reset   {| value = value; id = transactionId |}]
            | Add    delta -> [Events.Added   {| delta = delta; id = transactionId |}]
            | Remove delta when delta > openingBal -> []
            | Remove delta -> [Events.Removed {| delta = delta; id = transactionId |}]
        match events with
        | [] -> Denied, []
        | events ->
            let (Fold.Current updatedBal) = Fold.fold state events
            Accepted updatedBal, events

type Service internal (resolve : LocationId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Execute(locationId, transactionId, command) : Async<Result> =
        let stream = resolve locationId
        stream.Transact(decide transactionId command)

let create resolver maxAttempts =
    let resolve locationId =
        let stream = resolver (streamName locationId)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = maxAttempts)
    Service (resolve)

module Cosmos =

    let accessStrategy = Equinox.Cosmos.AccessStrategy.Unoptimized
    let resolver (context, cache) =
        let cacheStrategy = Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
    let create (context, cache, maxAttempts) =
        create (resolver (context, cache)) maxAttempts

module EventStore =

    open Equinox.EventStore

    let create (context, cache, maxAttempts) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy)
        create resolver.Resolve maxAttempts