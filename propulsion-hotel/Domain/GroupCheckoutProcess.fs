module Domain.GroupCheckoutProcess

let [<Literal>] Category = "GroupCheckoutTransaction"
let streamId = Equinox.StreamId.gen GroupCheckoutId.toString

[<AutoOpen>]
module Events =

    type CheckoutResidual =     { stay :  GuestStayId; residual : decimal }
    type Event =
        | StaysAdded of         {| at : DateTimeOffset; stays : GuestStayId[] |}
        | GuestsMerged of       {| stays : CheckoutResidual[] |}
        /// Guest was checked out via another group, or independently, prior to being able to grab it
        | GuestsFailed of       {| stays : GuestStayId[] |}
        | Paid of               {| at : DateTimeOffset; paymentId : PaymentId; amount : decimal |}
        | Confirmed of          {| at : DateTimeOffset |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Config.EventCodec.gen<Event>

[<AutoOpen>]
module Fold =

    [<NoComparison; NoEquality>]
    type State = { pending : GuestStayId[]; checkedOut : CheckoutResidual[]; failed : GuestStayId[]; balance : decimal; payments : PaymentId[]; completed : bool }

    let initial = { pending = [||]; checkedOut = [||]; failed = [||]; payments = [||]; balance = 0m; completed = false }

    let private removePending xs state = { state with pending = state.pending |> Array.except xs  }
    let evolve state = function
        | StaysAdded e ->       { state with pending = Array.append state.pending e.stays }
        | GuestsMerged e ->     { removePending (seq { for s in e.stays -> s.stay }) state with
                                             checkedOut = Array.append state.checkedOut e.stays
                                             balance = state.balance + (e.stays |> Seq.sumBy (fun x -> x.residual)) }
        | GuestsFailed e ->     { removePending e.stays state with
                                             failed = Array.append state.failed e.stays }
        | Paid e ->             { state with balance = state.balance - e.amount
                                             payments = [| yield! state.payments; e.paymentId |] }
        | Confirmed e ->        { state with completed = true }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

module Flow =

    type Action =
        | Checkout              of GuestStayId[]
        | Ready                 of balance : decimal
        | Finished

    let nextAction : Fold.State -> Action = function
        | { completed = true } -> Finished
        | { pending = xs } when not (Array.isEmpty xs) -> Checkout xs
        | { balance = bal } -> Ready bal

    let pendingFrom (stays : GuestStayId seq) state =
        let t = HashSet(stays)
        t.IntersectWith state.pending
        t

    // If there are no events to apply to the state, it pushes the transaction manager to follow up on the next action from where it was
    let decide (update : Events.Event option) (state : Fold.State) : Events.Event list =
        match update with
        | Some (Events.Event.GuestsFailed e) ->
            match state |> pendingFrom e.stays |> Array.ofSeq with
            | [||] -> []
            | xs -> [ Events.GuestsFailed {| stays = Array.ofSeq xs |} ]
        | Some (Events.Event.GuestsMerged e) ->
            let fresh = state |> pendingFrom (seq { for x in e.stays -> x.stay })
            match [| for x in e.stays do if fresh.Contains x.stay then x |] with
            | [||] -> []
            | xs -> [ Events.GuestsMerged {| stays = xs |} ]
        | Some x -> failwithf "unexpected %A" x
        | None -> []

module Decide =

    let add at stays state =
        let registered = HashSet(seq { yield! state.pending; yield! state.failed; for x in state.checkedOut do yield x.stay })
        match stays |> Array.except registered with
        | [||] -> []
        | xs -> [ Events.StaysAdded {| at = at; stays = xs |} ]

    let confirm at state =
        match Flow.nextAction state with
        | Flow.Action.Finished -> true, []
        | Flow.Action.Ready 0m -> true, [ Events.Confirmed {| at = at |} ]
        | Flow.Action.Ready _
        | Flow.Action.Checkout _ -> false, []

    let pay paymentId amount at = function
        | { payments = paymentIds } when paymentIds |> Array.contains paymentId -> []
        | { balance = bal } -> [ Events.Paid {| at = at; paymentId = paymentId; amount = amount |} ]

type Service internal (resolve : GroupCheckoutId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Step(transactionId, maybeUpdate) : Async<Flow.Action> =
        let decider = resolve transactionId
        decider.Transact(Flow.decide maybeUpdate, Flow.nextAction)

    member _.Add(id, stays, ?at) : Async<Flow.Action>=
        let decider = resolve id
        decider.Transact(Decide.add (defaultArg at DateTimeOffset.UtcNow) stays, Flow.nextAction)

    member _.Pay(id, paymentId, amount, ?at) : Async<unit> =
        let decider = resolve id
        decider.Transact(Decide.pay paymentId amount (defaultArg at DateTimeOffset.UtcNow))

    member _.Confirm(id, ?at) : Async<bool>=
        let decider = resolve id
        decider.Transact(Decide.confirm (defaultArg at DateTimeOffset.UtcNow))

module Config =

    let private (|Category|) = function
        | Config.Store.Memory store ->            Config.Memory.create Events.codec Fold.initial Fold.fold store
        | Config.Store.Dynamo (context, cache) ->
            // Not using snapshots, on the basis that the writes are all coming from this process, so the cache will be sufficient
            // to make reads cheap enough, with the benefit of writes being cheaper as you're not paying to maintain the snapshot
            Config.Dynamo.createUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
    let create store = streamId >> Config.resolve ((|Category|) store) Category |> Service
