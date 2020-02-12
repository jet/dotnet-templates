module Domain.Inventory.Epoch

open System

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] categoryId = "InventoryEpoch"
    let (|For|) (inventoryId : InventoryId, epochId : InventoryEpochId) =
        let streamId = sprintf "%s_%s" (InventoryId.toString inventoryId) (InventoryEpochId.toString epochId)
        Equinox.AggregateId(categoryId, streamId)

    type Transferred = { transactionId : InventoryTransactionId }
    type Adjusted = { transactionId : InventoryTransactionId }

    type Snapshotted = { closed: bool; ids : InventoryTransactionId[] }

    type Event =
        | Adjusted of Adjusted
        | Transferred of Transferred
        | Closed
        | Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    // TOCONSIDER - codec could pull transferId from meta

module Fold =

    type State = { closed : bool; ids : Set<InventoryTransactionId> }
    let initial = { closed = false; ids = Set.empty }
    let evolve state = function
        | Events.Adjusted e -> { state with ids = Set.add e.transactionId state.ids }
        | Events.Transferred e -> { state with ids = Set.add e.transactionId state.ids }
        | Events.Closed -> { state with closed = true }
        | Events.Snapshotted e -> { closed = e.closed; ids = Set.ofArray e.ids }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot s = Events.Snapshotted { closed = s.closed; ids = Array.ofSeq s.ids }

type Result =
    {   /// Indicates whether this epoch is closed (either previously or as a side-effect this time)
        isClosed : bool
        /// Count of items added to this epoch. May be less than requested due to removal of duplicates and/or rejected items
        added : int
        /// residual items that [are not duplicates and] were not accepted into this epoch
        rejected : Events.Event list }

let decideSync capacity requested (state : Fold.State) : Result * Events.Event list =
    let isFresh = function
        | Events.Adjusted { transactionId = id }
        | Events.Transferred { transactionId = id } -> (not << state.ids.Contains) id
        | Events.Closed | Events.Snapshotted _ -> false
    let news = requested |> Seq.filter isFresh |> List.ofSeq
    let closed,allowing,markClosed,residual =
        let newCount = List.length news
        if state.closed then
            true,0,false,news
        else
            let capacityNow = capacity state
            let accepting = min capacityNow newCount
            let closing = accepting = capacityNow
            let residual = List.skip accepting news
            closing,accepting,closing,residual
    let events =
        [ if allowing <> 0 then yield! news
          if markClosed then yield Events.Closed ]
    let state' = Fold.fold state events
    { isClosed = closed; added = allowing; rejected = residual },events

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.For streamId) = Equinox.Stream<Events.Event,Fold.State>(log, resolve streamId, maxAttempts)

    /// Attempt ingestion of `events` into the cited Epoch.
    /// - No `items` will be accepted if the Epoch is `closed`
    /// - The `capacity` function will be passed a non-closed `state` in order to determine number of items that can be admitted prior to closing
    /// - If the computed capacity result is >= the number of items being submitted (which may be 0), the Epoch will be marked Closed
    /// NOTE the result may include rejected items (which the caller is expected to feed into a successor epoch)
    member __.IngestShipped(inventoryId, epochId, capacity, events) : Async<Result> =
        let stream = resolve (inventoryId, epochId)
        stream.Transact(decideSync capacity events)

let createService resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 2)

module Cosmos =

    open Equinox.Cosmos
    let accessStrategy = Equinox.Cosmos.AccessStrategy.Snapshot (Fold.isOrigin, Fold.snapshot)
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
    let createService (context,cache) = createService (resolve (context,cache))