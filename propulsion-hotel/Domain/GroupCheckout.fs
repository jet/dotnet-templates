module Domain.GroupCheckout

module private Stream =
    let [<Literal>] Category = "GroupCheckout"
    let id = FsCodec.StreamId.gen GroupCheckoutId.toString
    let decodeId = FsCodec.StreamId.dec GroupCheckoutId.parse
    let tryDecode = FsCodec.StreamName.tryFind Category >> ValueOption.map decodeId

module Reactions =
    let [<Literal>] Category = Stream.Category
    let [<return: Struct>] (|For|_|) = Stream.tryDecode
    
module Events =

    type CheckoutResidual =     { stay:  GuestStayId; residual: decimal }
    type Event =
        /// There may be more than one of these; each represents the user requesting the adding a group of Stays into group checkout
        /// NOTE in the case where >=1 of the nominated Stays has already been checked out, the pending stay will be taken off the list via
        /// a MergesFailed event rather than the typical StaysMerged outcome
        | StaysSelected of      {| at: DateTimeOffset; stays: GuestStayId[] |}
        /// Represents the workflow's record of a) confirming checkout of the Stay has been completed and b) the balance to be paid, if any
        | StaysMerged of        {| residuals: CheckoutResidual[] |}
        /// Indicates that it was not possible for the the Selected stay to be transferred to the group as requested
        /// i.e. Guest was checked out via another group, or independently, prior to being able to grab it.
        | MergesFailed of       {| stays: GuestStayId[] |}
        /// Records payments for this group (a group cannot be confirmed until all outstanding charges for all Merged stays have Paid)
        | Paid of               {| at: DateTimeOffset; paymentId: PaymentId; amount: decimal |}
        /// Records confirmation of completion of the group checkout. No further Stays can be Selected, nor should any balance be outstanding
        | Confirmed of          {| at: DateTimeOffset |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>

module Fold =

    open Events
    
    [<NoComparison; NoEquality>]
    type State = { pending: GuestStayId[]; checkedOut: CheckoutResidual[]; failed: GuestStayId[]; balance: decimal; payments: PaymentId[]; completed: bool }

    let initial = { pending = [||]; checkedOut = [||]; failed = [||]; payments = [||]; balance = 0m; completed = false }

    let private removePending xs state = { state with pending = state.pending |> Array.except xs  }
    let evolve state = function
        | StaysSelected e ->
            { state with pending = Array.append state.pending e.stays }
        | StaysMerged e ->
            { removePending (seq { for s in e.residuals -> s.stay }) state with
                checkedOut = Array.append state.checkedOut e.residuals
                balance = state.balance + (e.residuals |> Seq.sumBy (fun x -> x.residual)) }
        | MergesFailed e ->
            { removePending e.stays state with
                failed = Array.append state.failed e.stays }
        | Paid e ->
            { state with
                balance = state.balance - e.amount
                payments = [| yield! state.payments; e.paymentId |] }
        | Confirmed _ ->
            { state with completed = true }
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

/// Manages the Workflow aspect, mapping Fold.State to an Action surfacing information relevant for reactions processing
module Flow =

    type State =
        | MergeStays of         GuestStayId[]
        | Ready of              balance: decimal
        | Finished

    let nextAction: Fold.State -> State = function
        | { completed = true } -> Finished
        | { pending = xs } when not (Array.isEmpty xs) -> MergeStays xs
        | { balance = bal } -> Ready bal
        
    let decide handleAction (state: Fold.State): Async<'R * Events.Event[]> =
        nextAction state |> handleAction

module Decide =

    open Fold
    
    let add at stays state =
        let registered = HashSet(seq { yield! state.pending; yield! state.failed; for x in state.checkedOut do yield x.stay })
        match stays |> Array.except registered with
        | [||] -> [||]
        | xs -> [| Events.StaysSelected {| at = at; stays = xs |} |]

    [<NoComparison; NoEquality>]
    type ConfirmResult = Processing | Ok | BalanceOutstanding of decimal
    let confirm at state =
        match Flow.nextAction state with
        | Flow.Finished -> ConfirmResult.Ok, [||]
        | Flow.Ready 0m -> ConfirmResult.Ok, [| Events.Confirmed {| at = at |} |]
        | Flow.Ready amount -> ConfirmResult.BalanceOutstanding amount, [||]
        | Flow.MergeStays _ -> ConfirmResult.Processing, [||]

    let pay paymentId amount at = function
        | { payments = paymentIds } when paymentIds |> Array.contains paymentId -> [||]
        | _ -> [| Events.Paid {| at = at; paymentId = paymentId; amount = amount |} |]

type Service internal (resolve: GroupCheckoutId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Merge(id, stays, ?at): Async<Flow.State>=
        let decider = resolve id
        decider.Transact(Decide.add (defaultArg at DateTimeOffset.UtcNow) stays, Flow.nextAction)

    member _.Pay(id, paymentId, amount, ?at): Async<unit> =
        let decider = resolve id
        decider.Transact(Decide.pay paymentId amount (defaultArg at DateTimeOffset.UtcNow))
        
    member _.Confirm(id, ?at): Async<Decide.ConfirmResult>=
        let decider = resolve id
        decider.Transact(Decide.confirm (defaultArg at DateTimeOffset.UtcNow))

    /// Used by GroupCheckOutProcess to run any relevant Reaction activities
    member _.React(id, handleReaction: Flow.State -> Async<'R * Events.Event[]>): Async<'R * int64> =
        let decider = resolve id
        decider.TransactWithPostVersion(Flow.decide handleReaction)

    member _.Read(groupCheckoutId): Async<Flow.State>=
        let decider = resolve groupCheckoutId
        decider.Query(Flow.nextAction)

module Factory =

    let private (|Category|) = function
        | Store.Config.Memory store ->
            Store.Memory.create Stream.Category Events.codec Fold.initial Fold.fold store
        | Store.Config.Dynamo (context, cache) ->
            Store.Dynamo.createUnoptimized Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
        | Store.Config.Mdb (context, cache) ->
            Store.Mdb.createUnoptimized Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Stream.id >> Store.createDecider cat |> Service
