namespace TodoBackendTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value : ClientId) : string = Guid.toStringN %value

[<AutoOpen>]
module DeciderExtensions =

    type Equinox.Decider<'e, 's> with

         // see https://github.com/jet/equinox/pull/320
         member x.Transact(decide, mapResult) =
            x.TransactEx((fun c -> async { let events = decide c.State in return (), events }), fun () c -> mapResult c.State)
