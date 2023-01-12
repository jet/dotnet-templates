namespace Domain

open FSharp.UMX
open System

module Guid =
    let toString (x : Guid) : string = x.ToString "N"

type GroupCheckoutId = Guid<groupCheckoutId>
 and [<Measure>] groupCheckoutId
module GroupCheckoutId =
    let toString : GroupCheckoutId -> string = UMX.untag >> Guid.toString
    let (|Parse|) : string -> GroupCheckoutId = Guid.Parse >> UMX.tag

type GuestStayId = Guid<guestStayId>
 and [<Measure>] guestStayId
module GuestStayId =
    let toString : GuestStayId -> string = UMX.untag >> Guid.toString

type ClerkId = Guid<clerkId>
 and [<Measure>] clerkId
 
type RequestId = Guid<requestId>
 and [<Measure>] requestId

type ChargeId = Guid<chargeId>
 and [<Measure>] chargeId

type PaymentId = Guid<paymentId>
 and [<Measure>] paymentId

type DateTimeOffset = System.DateTimeOffset
type HashSet<'t> = System.Collections.Generic.HashSet<'t>
