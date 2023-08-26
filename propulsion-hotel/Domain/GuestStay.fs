module Domain.GuestStay

module private Stream =
    let [<Literal>] Category = "GuestStay"
    let id = FsCodec.StreamId.gen GuestStayId.toString

module Events =

    type Event =
        /// Notes time of of checkin of the guest (does not affect whether charges can be levied against the stay)
        | CheckedIn of          {| at: DateTimeOffset |}
        /// Notes addition of a charge against the stay
        | Charged of            {| chargeId: ChargeId; at: DateTimeOffset; amount: decimal |}
        /// Notes a payment against this stay
        | Paid of               {| paymentId: PaymentId; at: DateTimeOffset; amount: decimal |}
        /// Notes an ordinary checkout by the Guest (requires prior payment of all outstanding charges)
        | CheckedOut of         {| at: DateTimeOffset |}
        /// Notes checkout is being effected via a GroupCheckout. Marks stay complete equivalent to typical CheckedOut event
        | TransferredToGroup of {| at: DateTimeOffset; groupId: GroupCheckoutId; residualBalance: decimal |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Store.Codec.gen<Event>

module Fold =

    [<NoComparison; NoEquality>]
    type State =
        | Active of             Balance
        | Closed
        | TransferredToGroup of {| groupId: GroupCheckoutId; amount: decimal |}
    and Balance = { balance: decimal; charges: ChargeId[]; payments: PaymentId[]; checkedInAt: DateTimeOffset option }
    let initial = Active { balance = 0m; charges = [||]; payments = [||]; checkedInAt = None }

    let evolve state event =
        match state with
        | Active bal ->
            match event with
            | Events.CheckedIn e -> Active { bal with checkedInAt = Some e.at }
            | Events.Charged e ->   Active { bal with balance = bal.balance + e.amount; charges = [| yield! bal.charges; e.chargeId |] }
            | Events.Paid e ->      Active { bal with balance = bal.balance - e.amount; payments = [| yield! bal.payments; e.paymentId |] }
            | Events.CheckedOut _ -> Closed
            | Events.TransferredToGroup e -> TransferredToGroup {| groupId = e.groupId; amount = e.residualBalance |}
        | Closed _ | TransferredToGroup _ -> invalidOp "No events allowed after CheckedOut/TransferredToGroup"
    let fold: State -> Events.Event seq -> State = Seq.fold evolve

module Decide =

    open Fold
    
    let checkin at = function
        | Active { checkedInAt = None } -> [ Events.CheckedIn {| at = at |}  ]
        | Active { checkedInAt = Some t } when t = at -> []
        | Active _ | Closed _ | TransferredToGroup _ -> invalidOp "Invalid checkin"

    let charge at chargeId amount = function
        | Closed _ | TransferredToGroup _ -> invalidOp "Cannot record charge for Closed account"
        | Active bal ->
            if bal.charges |> Array.contains chargeId then [||]
            else [| Events.Charged {| at = at; chargeId = chargeId; amount = amount |} |]

    let payment at paymentId amount = function
        | Closed _ | TransferredToGroup _ -> invalidOp "Cannot record payment for not opened account" // TODO fix message at source
        | Active bal ->
            if bal.payments |> Array.contains paymentId then [||]
            else [| Events.Paid {| at = at; paymentId = paymentId; amount = amount |} |]

    [<RequireQualifiedAccess>]
    type CheckoutResult = Ok | AlreadyCheckedOut | BalanceOutstanding of decimal
    let checkout at: State -> CheckoutResult * Events.Event[] = function
        | Closed -> CheckoutResult.Ok, [||]
        | TransferredToGroup _ -> CheckoutResult.AlreadyCheckedOut, [||]
        | Active { balance = 0m } -> CheckoutResult.Ok, [| Events.CheckedOut {| at = at |} |]
        | Active { balance = residual } -> CheckoutResult.BalanceOutstanding residual, [||]

    [<RequireQualifiedAccess>]
    type GroupCheckoutResult = Ok of residual: decimal | AlreadyCheckedOut
    let groupCheckout at groupId: State -> GroupCheckoutResult * Events.Event[] = function
        | Closed -> GroupCheckoutResult.AlreadyCheckedOut, [||]
        | TransferredToGroup s when s.groupId = groupId -> GroupCheckoutResult.Ok s.amount, [||]
        | TransferredToGroup _ -> GroupCheckoutResult.AlreadyCheckedOut, [||]
        | Active { balance = residual } -> GroupCheckoutResult.Ok residual, [| Events.TransferredToGroup {| at = at; groupId = groupId; residualBalance = residual |} |]

type Service internal (resolve: GuestStayId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Charge(id, chargeId, amount) =
        let decider = resolve id
        decider.Transact(Decide.charge DateTimeOffset.UtcNow chargeId amount)
 
    member _.Pay(id, paymentId, amount) =
        let decider = resolve id
        decider.Transact(Decide.payment DateTimeOffset.UtcNow paymentId amount)
 
    member _.Checkout(id, at): Async<Decide.CheckoutResult> =
        let decider = resolve id
        decider.Transact(Decide.checkout (defaultArg at DateTimeOffset.UtcNow))

    // Driven exclusively by GroupCheckout
    member _.GroupCheckout(id, groupId, ?at): Async<Decide.GroupCheckoutResult> =
        let decider = resolve id
        decider.Transact(Decide.groupCheckout (defaultArg at DateTimeOffset.UtcNow) groupId)

module Factory =

    let private (|Category|) = function
        | Store.Config.Memory store ->
            Store.Memory.create Stream.Category Events.codec Fold.initial Fold.fold store
        | Store.Config.Dynamo (context, cache) ->
            Store.Dynamo.createUnoptimized Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
        | Store.Config.Mdb (context, cache) ->
            Store.Mdb.createUnoptimized Stream.Category Events.codec Fold.initial Fold.fold (context, cache)
    let create (Category cat) = Service(Stream.id >> Store.createDecider cat)
