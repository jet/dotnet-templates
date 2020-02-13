module Fc.InventoryTransaction

open System

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] CategoryId = "Transfer"
    let (|For|) (inventoryId, transactionId) =
        FsCodec.StreamName.compose CategoryId [InventoryId.toString inventoryId; InventoryTransactionId.toString transactionId]

    type TransferRequested = { source : LocationId; destination : LocationId; quantity : int }
    type Removed = { balance : int }
    type Added = { balance : int }
    type AdjustmentRequested = { location : LocationId; quantity : int }

    type Event =
        | AdjustmentRequested of AdjustmentRequested
        | Adjusted
        | TransferRequested of TransferRequested
        | Failed
        | Removed of Removed
        | Added of Added
        | Completed
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type Removed = { request : Events.TransferRequested; removed : Events.Removed }
    type Added = { request : Events.TransferRequested; removed : Events.Removed; added : Events.Added }
    type State =
        | Initial
        | Adjusting of Events.AdjustmentRequested
        | Adjusted of Events.AdjustmentRequested
        | Transferring of Events.TransferRequested
        | Failed
        | Adding of Removed
        | Added of Added
        | Completed
    let initial = Initial
    let evolve state = function
        | Events.AdjustmentRequested e -> Adjusting e
        | Events.Adjusted as ee ->
            match state with
            | Adjusting s -> Adjusted s
            | x -> failwithf "Unexpected %A when %A " ee state
        | Events.TransferRequested e -> Transferring e
        | Events.Failed -> Failed
        | Events.Removed e as ee ->
            match state with
            | Transferring s -> Adding { request = s; removed = e  }
            | x -> failwithf "Unexpected %A when %A " ee state
        | Events.Added e as ee ->
            match state with
            | Adding s -> Added { request = s.request; removed = s.removed; added = e  }
            | x -> failwithf "Unexpected %A when %A " ee state
        | Events.Completed -> Completed
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

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
    let resolve (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
    let createService (context, cache) = createService (resolve (context,cache))