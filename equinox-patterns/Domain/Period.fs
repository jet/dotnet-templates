/// Illustrates a high level approach to how one might manage a chained series of periods which can be logically Closed
/// All write attempts adhere to a common protocol to effect this semantic:-
/// a) all preceding periods must be closed, idempotently i) computing and persisting a balance OR ii) honoring a previously recorded one
/// b) the decision is processed within the target period (which may be either Open, or being opened as part of this flow)
/// c) if appropriate, the target period may be closed as part of the same decision flow if `decideCarryForward` yields Some
module Patterns.Domain.Period

let [<Literal>] Category = "Period"
let streamName periodId = FsCodec.StreamName.create Category (PeriodId.toString periodId)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemIds = { items : ItemId[] }
    type Balance = ItemIds
    type Event =
        | BroughtForward    of Balance
        | Added             of ItemIds
        | CarriedForward    of Balance
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State =
        | Initial
        | Open of           items : OpenState
        | Closed of         items : ItemId[] * carryingForward : ItemId[]
     and OpenState = ItemId[]
    let initial : State = Initial
    let (|Items|) = function Initial -> [||] | Open i | Closed (i, _) -> i
    open Events
    let evolve (Items items) = function
        | BroughtForward e
        | Added e ->        Open (Array.append items e.items)
        | CarriedForward e -> Closed (items, e.items)
    let fold = Seq.fold evolve

    /// Handles one-time opening of the Period, if applicable
    let maybeOpen (getIncomingBalance : unit -> Async<Balance>) state = async {
        match state with
        | Initial ->        let! balance = getIncomingBalance ()
                            return [BroughtForward balance]
        | Open _
        | Closed _ ->       return [] }

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
        /// balance being carried forward in the event that the successor period has yet to have the BroughtForward event generated
        carryForward        : Events.Balance option }

/// Decision function ensuring the high level rules of an Period are adhered to viz.
/// 1. Streams must open with a BroughtForward event (obtained via Rules.getIncomingBalance if this is an uninitialized Period)
/// 2. (If the Period has not closed) Rules.decide gets to map the request to events and a residual
/// 3. Rules.decideCarryForward may trigger the closing of the Period based on the residual and/or the State by emitting Some balance
let decideIngestWithCarryForward rules req s : Async<Result<'result, 'req> * Events.Event list> = async {
    let acc = Accumulator(s, Fold.fold)
    do! acc.TransactAsync(Fold.maybeOpen rules.getIncomingBalance)
    let residual, result = acc.Transact(Fold.tryIngest rules.decideIngestion req)
    let! residual, carryForward = acc.TransactAsync(Fold.maybeClose rules.decideCarryForward residual)
    return { residual = residual; result = result; carryForward = carryForward }, acc.Events
}

/// Manages Application of Requests to the Period's stream, including closing preceding periods as appropriate
type Service internal (resolve : PeriodId -> Equinox.Decider<Events.Event, Fold.State>) =

    let calcBalance state =
        let createEventsBalance items : Events.Balance = { items = items }
        async { return createEventsBalance state }
    let genBalance state = async { let! bal = calcBalance state in return Some bal }

    /// Walks back as far as necessary to ensure any preceding Periods that are not yet Closed are, then closes the target if necessary
    /// Yields the accumulated balance to be carried forward into the next period
    let rec close periodId : Async<Events.Balance> =
        let rules : Rules<unit, unit> =
            {   getIncomingBalance  = fun ()        -> close periodId
                decideIngestion     = fun () _state -> (), (), []
                decideCarryForward  = fun ()        -> genBalance } // always close
        let decider = resolve periodId
        decider.TransactEx((fun c -> decideIngestWithCarryForward rules () c.State), fun r _c -> Option.get r.carryForward)

    /// Runs the decision function on the specified Period, closing and bringing forward balances from preceding Periods if necessary
    let tryTransact periodId getIncoming (decide : 'request -> Fold.State -> 'request * 'result * Events.Event list) request shouldClose : Async<Result<'request, 'result>> =
        let rules : Rules<'request, 'result> =
            {   getIncomingBalance  = getIncoming
                decideIngestion     = fun request state -> let residual, result, events = decide request state in residual, result, events
                decideCarryForward  = fun res state -> async { if shouldClose res then return! genBalance state else return None } } // also close, if we should
        let decider = resolve periodId
        decider.TransactEx((fun c -> decideIngestWithCarryForward rules request c.State), fun r _c -> r)

    /// Runs the decision function on the specified Period, closing and bringing forward balances from preceding Periods if necessary
    /// Processing completes when `decide` yields None for the residual of the 'request
    member _.Transact(periodId, decide : 'request -> Fold.State -> 'request option * 'result * Events.Event list, request : 'request) : Async<'result> =
        let rec aux periodId getIncoming req = async {
            let decide req state = decide (Option.get req) state
            match! tryTransact periodId getIncoming decide req Option.isSome with
            | { residual = None; result = Some r } -> return r
            | { residual = r; carryForward = cf } -> return! aux (PeriodId.next periodId) (fun () -> async { return Option.get cf }) r }
        let getIncoming () =
            match PeriodId.tryPrev periodId with
            | None -> calcBalance [||]
            | Some prevPeriodId -> close prevPeriodId
        aux periodId getIncoming (Some request)

    /// Exposes the full state to a reader (which is appropriate for a demo but is an anti-pattern in the general case)
    /// NOTE the resolve function below uses Equinox.AllowStale - you may want to remove that option of you write a read
    ///      function like this for real (i.e. if the active Period is constantly being appended to in another process/machine)
    ///      then this read function will continually serve the same value, as it has been instructed not to make a store round-trio
    member _.Read periodId =
        let decider = resolve periodId
        decider.Query id

let private create resolveStream =
    let resolve = streamName >> resolveStream (Some Equinox.AllowStale) >> Equinox.createDecider
    Service resolve

module MemoryStore =

    let create store =
        let cat = Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create resolveStream

module Cosmos =

    open Equinox.CosmosStore

    // Not using snapshots, on the basis that the writes are all coming from this process, so the cache will be sufficient
    // to make reads cheap enough, with the benefit of writes being cheaper as you're not paying to maintain the snapshot
    let accessStrategy = AccessStrategy.Unoptimized
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let cat = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        let resolveStream opt sn = cat.Resolve(sn, ?option = opt)
        create resolveStream
