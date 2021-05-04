/// Illustrates a high level approach to how one might manage a chained series of epochs which can be logically Closed
/// When the target Epoch is Closed, all write attempts attempts are required to adhere to a protocol consisting of
/// a) all preceding epochs must be closed, idempotently computing and persisting or honoring a previous computed balance
/// b) the decision is processed within the target epoch (which may be either Open, or being opened as part of this flow)
/// c) if appropriate, the target epoch may be closed as part of the same decision flow if `decideCarryForward` yields Some
module Patterns.Domain.Epoch

let [<Literal>] Category = "Epoch"
let streamName epochId = FsCodec.StreamName.create Category (EpochId.toString epochId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemIds = { items : string[] }
    type Balance = ItemIds
    type Event =
        | BroughtForward of Balance
        | Added of ItemIds
        | Removed of ItemIds
        | CarriedForward of Balance
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State =
        | Initial
        | Open of items : OpenState
        | Closed of items : string[] * carryingForward : string[]
     and OpenState = string[]
    let initial : State = Initial
    let (|Items|) = function Initial -> [||] | Open i | Closed (i, _) -> i
    open Events
    let evolve (Items items) = function
        | BroughtForward e
        | Added e ->        Open (Array.append items e.items)
        | Removed e ->      Open (items |> Array.except e.items)
        | CarriedForward e -> Closed (items, e.items)
    let fold = Seq.fold evolve

    /// Handles one-time opening of the Epoch, if applicable
    let maybeOpen (getIncomingBalance : unit -> Async<Balance>) state = async {
        match state with
        | Initial ->        let! balance = getIncomingBalance ()
                            return (), [BroughtForward balance]
        | Open _
        | Closed _ ->       return (), [] }

    /// Handles attempting to apply the request to the stream (assuming it's not already closed)
    /// The `decide` function can signal a need to close and/or split the request by emitting it as the residual
    let tryIngest (decide : 'req -> State -> 'req * 'result * Event list) req = function
        | Initial ->        failwith "Invalid tryIngest; stream not Open"
        | Open _ as s ->    let residual, result, events = decide req s
                            (residual, Some result), events
        | Closed _ ->       (req, None), []

    /// Yields or computes the Balance to be Carried forward and/or application of the event representing that decision
    let maybeClose (decideCarryForward : 'residual -> OpenState -> Async<Balance option>) residual state = async {
        match state with
        | Initial ->        return failwith "Invalid maybeClose; stream not Open"
        | Open s ->         let! cf = decideCarryForward residual s
                            let events = cf |> Option.map CarriedForward |> Option.toList
                            return (residual, cf), events
        | Closed (_, b) ->  return (residual, Some { items = b }), [] }

[<NoComparison; NoEquality>]
type Rules<'request, 'result> =
    {   getIncomingBalance  : unit -> Async<Events.Balance>
        decideIngestion     : 'request -> Fold.State -> 'request * 'result * Events.Event list
        decideCarryForward  : 'request -> Fold.OpenState -> Async<Events.Balance option> }

/// The result of the overall ingestion, consisting of
type Result<'request, 'result> =
    {   /// residual of the request, in the event where it was not possible to ingest it completely
        residual            : 'request
        /// The result of the decision (assuming processing took place)
        result              : 'result option
        /// balance being carried forward in the event that the successor epoch has yet to have the BroughtForward event generated
        carryForward        : Events.Balance option }

/// Decision function ensuring the high level rules of an Epoch are adhered to viz.
/// 1. Streams must open with a BroughtForward event (obtained via Rules.getIncomingBalance if this is an uninitialized Epoch)
/// 2. (If the Epoch has not closed) Rules.decide gets to map the request to events and a residual
/// 3. Rules.decideCarryForward may trigger the closing of the Epoch based on the residual and the stream State by emitting Some balance
let decideIngestWithCarryForward rules req s : Async<Result<'result, 'req> * Events.Event list> = async {
    let acc = Accumulator(s, Fold.fold)
    do! acc.TransactAsync(Fold.maybeOpen rules.getIncomingBalance)
    let residual, result = acc.Transact(Fold.tryIngest rules.decideIngestion req)
    let! residual, carryForward = acc.TransactAsync(Fold.maybeClose rules.decideCarryForward residual)
    return { residual = residual; result = result; carryForward = carryForward }, acc.Events
}

/// Manages Application of Requests to the Epoch's stream, including closing preceding epochs as appropriate
type Service(resolve : EpochId -> Equinox.Decider<Events.Event, Fold.State>) =

    let calcBalance state =
        let createEventsBalance items : Events.Balance = { items = items }
        async { return createEventsBalance state }

    /// Walks back as far as necessary to ensure any preceding Epochs that are not yet Closed are, then closes the target if necessary
    /// Yields the accumulated balance to be carried forward into the next epoch
    member private x.Close epochId : Async<Events.Balance> =
        let rules : Rules<unit, unit> =
            {   getIncomingBalance  = fun ()        -> x.Close epochId
                decideIngestion     = fun () _state -> (), (), []
                decideCarryForward  = fun () state  -> async { let! bal = calcBalance state in return Some bal } } // always close
        let decider = resolve epochId
        decider.TransactEx((fun c -> decideIngestWithCarryForward rules () c.State), fun r _c -> Option.get r.carryForward)

    /// Runs the decision function on the specified Epoch, closing and bringing forward balances from preceding Epochs if necessary
    member private x.TryTransact(epochId, getIncoming, decide : 'request -> Fold.State -> 'request * 'result * Events.Event list, request) : Async<Result<'request, 'result>> =
        let rules : Rules<'request, 'result> =
            {   getIncomingBalance  = getIncoming
                decideIngestion     = fun request state  -> let residual, result, events = decide request state in residual, result, events
                decideCarryForward  = fun _res _state -> async { return None } } // never close
        let decider = resolve epochId
        decider.TransactEx((fun c -> decideIngestWithCarryForward rules request c.State), fun r _c -> r)

    /// Runs the decision function on the specified Epoch, closing and bringing forward balances from preceding Epochs if necessary
    /// Processing completes when `decide` yields None for the residual of the 'request
    member x.Transact(epochId, decide : 'request -> Fold.State -> 'request option * 'result * Events.Event list, request : 'request) : Async<'result> =
        let rec aux epochId getIncoming request = async {
            let decide req state =
                let residual, result, es = decide (Option.get req) state
                residual, result, es
            match! x.TryTransact(epochId, getIncoming, decide, request) with
            | { residual = None; result = Some r } -> return r
            | { residual = r; carryForward = cf } -> return! aux (EpochId.next epochId) (fun () -> async { return Option.get cf }) r }
        let getIncoming () =
            match EpochId.tryPrev epochId with
            | None -> calcBalance [||]
            | Some prevEpochId -> x.Close prevEpochId
        aux epochId getIncoming (Some request)

    /// Exposes the full state to a reader (which is appropriate for a demo but is an anti-pattern in the general case)
    member _.Read epochId =
         let decider = resolve epochId
         decider.Query id

let private create resolveStream =
    let resolve id = Equinox.Decider(Serilog.Log.ForContext<Service>(), streamName id |> resolveStream, maxAttempts = 3)
    Service(resolve)

module MemoryStore =

    let create store =
        let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        create cat.Resolve
