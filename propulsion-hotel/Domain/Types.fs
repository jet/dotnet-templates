namespace Domain

open FSharp.UMX
open System

module Guid =
    let toString (x: Guid): string = x.ToString "N"

type GroupCheckoutId = Guid<groupCheckoutId>
 and [<Measure>] groupCheckoutId
module GroupCheckoutId =
    let toString: GroupCheckoutId -> string = UMX.untag >> Guid.toString
    let parse: string -> GroupCheckoutId = Guid.Parse >> UMX.tag

type GuestStayId = Guid<guestStayId>
 and [<Measure>] guestStayId
module GuestStayId =
    let toString: GuestStayId -> string = UMX.untag >> Guid.toString

type ChargeId = Guid<chargeId>
 and [<Measure>] chargeId

type PaymentId = Guid<paymentId>
 and [<Measure>] paymentId

type DateTimeOffset = System.DateTimeOffset
type HashSet<'t> = System.Collections.Generic.HashSet<'t>

/// Handles symmetric generation and decoding of StreamNames composed of a series of elements via the FsCodec.StreamId helpers
type internal CategoryId<'elements>(name, gen: 'elements -> FsCodec.StreamId, dec: FsCodec.StreamId -> 'elements) =
    member _.StreamName = gen >> FsCodec.StreamName.create name
    member _.TryDecode = FsCodec.StreamName.tryFind name >> ValueOption.map dec

[<AutoOpen>]
module DeciderExtensions =
 
    type Equinox.Decider<'E, 'S> with

        member x.TransactWithPostVersion(decide: 'S -> Async<'R * 'E[]>): Async<'R * int64> =
            x.TransactEx((fun c -> decide c.State),
                         (fun r (c: Equinox.ISyncContext<'S>) -> (r, c.Version)))
