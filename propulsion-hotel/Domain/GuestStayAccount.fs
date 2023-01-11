module Domain.GuestStayAccount

let [<Literal>] Category = "GuestStayAccount"
let streamId = Equinox.StreamId.gen GuestStayId.toString

[<AutoOpen>]
module Events =

    type Event =
        | CheckedIn of          {| at : DateTimeOffset |}
        | Charged of            {| chargeId : ChargeId; at : DateTimeOffset; amount : decimal |}
        | Paid of               {| paymentId : PaymentId; at : DateTimeOffset; amount : decimal |}
        | CheckedOut of         {| at : DateTimeOffset |}
        | TransferredToGroup of {| at : DateTimeOffset; groupId : GroupCheckoutId; residualBalance : decimal |}
        interface TypeShape.UnionContract.IUnionContract
    let codec = Config.EventCodec.gen<Event>

[<AutoOpen>]
module Fold =

    [<NoComparison; NoEquality>]
    type State =
        | Open of Balance
        | Closed
        | TransferredToGroup of {| groupId : GroupCheckoutId; amount : decimal |}
    and Balance = { balance : decimal; charges : ChargeId[]; payments : PaymentId[]; checkedInAt : DateTimeOffset option }
    let initial = Open { balance = 0m; charges = [||]; payments = [||]; checkedInAt = None }

    let evolve state event =
        match state with
        | Open bal ->
            match event with
            | Events.CheckedIn e -> Open { bal with checkedInAt = Some e.at }
            | Events.Charged e ->   Open { bal with balance = bal.balance + e.amount; charges = [| yield! bal.charges; e.chargeId |] }
            | Events.Paid e ->      Open { bal with balance = bal.balance - e.amount; charges = [| yield! bal.payments; e.paymentId |] }
            | Events.CheckedOut _ -> Closed
            | Events.TransferredToGroup e -> TransferredToGroup {| groupId = e.groupId; amount = e.residualBalance |}
        | Closed _ | TransferredToGroup _ -> invalidOp "No events allowed after CheckedOut/TransferredToGroup"
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

module Decide =

    let checkin at = function
        | Open { checkedInAt = None } -> [ Events.CheckedIn {| at = at |}  ]
        | Open { checkedInAt = Some t } when t = at -> []
        | Open _ | Closed _ | TransferredToGroup _ -> invalidOp "Invalid checkin"

    let charge at chargeId amount state =
        match state with
        | Closed _ | TransferredToGroup _ -> invalidOp "Cannot record charge for Closed account"
        | Open bal ->
            if bal.charges |> Array.contains chargeId then []
            else [ Events.Charged {| at = at; chargeId = chargeId; amount = amount |} ]

    let payment at paymentId amount = function
        | Closed _ | TransferredToGroup _ -> invalidOp "Cannot record payment for not opened account" // TODO fix message at source
        | Open bal ->
            if bal.payments |> Array.contains paymentId then []
            else [ Events.Paid {| at = at; paymentId = paymentId; amount = amount |} ]

    [<RequireQualifiedAccess>]
    type CheckoutResult = Ok | AlreadyCheckedOut | BalanceOutstanding of decimal
    let checkout at : State -> CheckoutResult * Events.Event list = function
        | Closed -> CheckoutResult.Ok, []
        | TransferredToGroup _ -> CheckoutResult.AlreadyCheckedOut, []
        | Open { balance = 0m } -> CheckoutResult.Ok, [ Events.CheckedOut {| at = at |} ]
        | Open { balance = residual } -> CheckoutResult.BalanceOutstanding residual, []

    [<RequireQualifiedAccess>]
    type GroupCheckoutResult = Ok of residual : decimal | AlreadyCheckedOut
    let groupCheckout at groupId : State -> GroupCheckoutResult * Events.Event list = function
        | Closed -> GroupCheckoutResult.AlreadyCheckedOut, []
        | TransferredToGroup s when s.groupId = groupId -> GroupCheckoutResult.Ok s.amount, []
        | TransferredToGroup _ -> GroupCheckoutResult.AlreadyCheckedOut, []
        | Open { balance = residual } -> GroupCheckoutResult.Ok residual, [ Events.TransferredToGroup {| at = at; groupId = groupId; residualBalance = residual |} ]

type Service internal (resolve : GroupCheckoutId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Charge(id, chargeId, amount) =
        let decider = resolve id
        decider.Transact(Decide.charge DateTimeOffset.UtcNow chargeId amount)
 
    member _.Checkout(id, at) : Async<Decide.CheckoutResult> =
        let decider = resolve id
        decider.Transact(Decide.checkout (defaultArg at DateTimeOffset.UtcNow))

    // Driven exclusively by GroupCheckoutProcess
    member _.GroupCheckout(id, groupId, ?at) : Async<Decide.GroupCheckoutResult> =
        let decider = resolve id
        decider.Transact(Decide.groupCheckout (defaultArg at DateTimeOffset.UtcNow) groupId)

module Config =

    let private (|StoreCat|) = function
        | Config.Store.Memory store ->            Config.Memory.create Events.codec Fold.initial Fold.fold store
        | Config.Store.Dynamo (context, cache) ->
            // Not using snapshots, on the basis that the writes are all coming from this process, so the cache will be sufficient
            // to make reads cheap enough, with the benefit of writes being cheaper as you're not paying to maintain the snapshot
            Config.Dynamo.createUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
    let create (StoreCat cat) = Service(streamId >> Config.resolve cat Category)
