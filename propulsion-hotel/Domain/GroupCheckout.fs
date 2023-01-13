module Domain.GroupCheckout

let [<Literal>] Category = "GroupCheckout"
let streamId = Equinox.StreamId.gen GroupCheckoutId.toString
let [<return: Struct>] (|StreamName|_|) = function
    | FsCodec.StreamName.CategoryAndId (Category, GroupCheckoutId.Parse id) -> ValueSome id
    | _ -> ValueNone

module Events =

    type CheckoutResidual =     { stay :  GuestStayId; residual : decimal }
    type Event =
        | StaysAdded of         {| at : DateTimeOffset; stays : GuestStayId[] |}
        | StaysMerged of        {| residuals : CheckoutResidual[] |}
        /// Guest was checked out via another group, or independently, prior to being able to grab it
        | MergesFailed of       {| stays : GuestStayId[] |}
        | Paid of               {| at : DateTimeOffset; paymentId : PaymentId; amount : decimal |}
        | Confirmed of          {| at : DateTimeOffset |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Config.EventCodec.gen<Event>

module Fold =

    open Events
    
    [<NoComparison; NoEquality>]
    type State = { pending : GuestStayId[]; checkedOut : CheckoutResidual[]; failed : GuestStayId[]; balance : decimal; payments : PaymentId[]; completed : bool }

    let initial = { pending = [||]; checkedOut = [||]; failed = [||]; payments = [||]; balance = 0m; completed = false }

    let private removePending xs state = { state with pending = state.pending |> Array.except xs  }
    let evolve state = function
        | StaysAdded e ->
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
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

module Flow =

    type Action =
        | MergeStays of           GuestStayId[]
        | Ready of              balance : decimal
        | Finished

    let nextAction : Fold.State -> Action = function
        | { completed = true } -> Finished
        | { pending = xs } when not (Array.isEmpty xs) -> MergeStays xs
        | { balance = bal } -> Ready bal
        
    let decide handleAction (state : Fold.State) : Async<'R * Events.Event list> =
        nextAction state |> handleAction

module Decide =

    open Fold
    
    let add at stays state =
        let registered = HashSet(seq { yield! state.pending; yield! state.failed; for x in state.checkedOut do yield x.stay })
        match stays |> Array.except registered with
        | [||] -> []
        | xs -> [ Events.StaysAdded {| at = at; stays = xs |} ]

    [<NoComparison; NoEquality>]
    type ConfirmResult = Processing | Ok | BalanceOutstanding of decimal
    let confirm at state =
        match Flow.nextAction state with
        | Flow.Finished -> ConfirmResult.Ok, []
        | Flow.Ready 0m -> ConfirmResult.Ok, [ Events.Confirmed {| at = at |} ]
        | Flow.Ready amount -> ConfirmResult.BalanceOutstanding amount, []
        | Flow.MergeStays _ -> ConfirmResult.Processing, []

    let pay paymentId amount at = function
        | { payments = paymentIds } when paymentIds |> Array.contains paymentId -> []
        | _ -> [ Events.Paid {| at = at; paymentId = paymentId; amount = amount |} ]

type Service internal (resolve : GroupCheckoutId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Called within Reactor host to Dispatch any relevant Reaction activities
    member _.React(id, handleReaction) : Async<'R * int64> =
        let decider = resolve id
        decider.TransactExAsync((fun c -> Flow.decide handleReaction c.State), fun r c -> (r, c.Version))

    member _.Merge(id, stays, ?at) : Async<Flow.Action>=
        let decider = resolve id
        decider.Transact(Decide.add (defaultArg at DateTimeOffset.UtcNow) stays, Flow.nextAction)

    member _.Pay(id, paymentId, amount, ?at) : Async<unit> =
        let decider = resolve id
        decider.Transact(Decide.pay paymentId amount (defaultArg at DateTimeOffset.UtcNow))

    member _.Confirm(id, ?at) : Async<Decide.ConfirmResult>=
        let decider = resolve id
        decider.Transact(Decide.confirm (defaultArg at DateTimeOffset.UtcNow))

module Config =

    let private (|StoreCat|) = function
        | Config.Store.Memory store ->            Config.Memory.create Events.codec Fold.initial Fold.fold store
        | Config.Store.Dynamo (context, cache) ->
            // Not using snapshots, on the basis that the writes are all coming from this process, so the cache will be sufficient
            // to make reads cheap enough, with the benefit of writes being cheaper as you're not paying to maintain the snapshot
            Config.Dynamo.createUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
    let create (StoreCat cat) = streamId >> Config.resolve cat Category |> Service
