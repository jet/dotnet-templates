namespace Domain

open FSharp.UMX

type GroupCheckoutId = Guid<groupCheckoutId>
 and [<Measure>] groupCheckoutId
module GroupCheckoutId =
    let toString: GroupCheckoutId -> string = UMX.untag >> Guid.toStringN
    let parse: string -> GroupCheckoutId = Guid.parse >> UMX.tag

type GuestStayId = Guid<guestStayId>
 and [<Measure>] guestStayId
module GuestStayId =
    let toString: GuestStayId -> string = UMX.untag >> Guid.toStringN

type ChargeId = Guid<chargeId>
 and [<Measure>] chargeId

type PaymentId = Guid<paymentId>
 and [<Measure>] paymentId
