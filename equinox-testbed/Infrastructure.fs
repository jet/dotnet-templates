[<AutoOpen>]
module TestbedTemplate.Infrastructure

open FSharp.UMX
open System

type Exception with
    // https://github.com/fsharp/fslang-suggestions/issues/660
    member this.Reraise () =
        (System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture this).Throw ()
        Unchecked.defaultof<_>

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId = let toStringN (value : ClientId) : string = Guid.toStringN %value

/// SkuId strongly typed id; represented internally as a Guid
// NB Perf is suboptimal as a key, see Equinox's samples/Store for expanded version
type SkuId = Guid<skuId>
and [<Measure>] skuId
module SkuId = let toStringN (value : SkuId) : string = Guid.toStringN %value