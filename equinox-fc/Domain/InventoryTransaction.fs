/// Process Manager used to:
/// - Coordinate competing attempts to transfer quantities from stock; if balance is 3 one of contesting requests to remove 2 or 3 items must reach `Failed`
/// - maintain rolling balance of stock levels per Location
/// - while recording any transfers or adjustment in an overall Inventory record
/// The Process is driven by two actors:
/// 1) The 'happy path', where a given actor is executing the known steps of the command flow
///    In the normal case, such an actor will bring the flow to a terminal state (Completed or Failed)
/// 2) A watchdog-projector, which reacts to observed events in this Category by stepping in to complete in-flight requests that have stalled
///    This represents the case where a 'happy path' actor died, or experienced another impediment on the path.
module Fc.InventoryTransaction

open System

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    let [<Literal>] CategoryId = "InventoryTransfer"
    let (|For|) (inventoryId, transactionId) = FsCodec.StreamName.create CategoryId (InventoryTransactionId.toString transactionId)

    type AdjustmentRequested = { location : LocationId; quantity : int }
    type TransferRequested = { source : LocationId; destination : LocationId; quantity : int }
    type Removed = { balance : int }
    type Added = { balance : int }

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

type Command =
    | Adjust of Events.AdjustmentRequested
    | Transfer of Events.TransferRequested
