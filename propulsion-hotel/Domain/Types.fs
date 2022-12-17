namespace Domain

open System

module Guid =
    let toString (x : Guid) : string = x.ToString "N"

type GroupCheckoutId = Guid
module GroupCheckoutId =
    let toString = Guid.toString

type GuestStayId = Guid
module GuestStayId =
    let toString = Guid.toString

type ClerkId = Guid
type RequestId = Guid
type ChargeId = RequestId
type PaymentId = RequestId
type DateTimeOffset = System.DateTimeOffset
type HashSet<'t> = System.Collections.Generic.HashSet<'t>
