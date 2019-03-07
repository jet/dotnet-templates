[<AutoOpen>]
module TestbedTemplate.Types

open FSharp.UMX
open System

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
open FSharp.UMX
module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId = let toStringN (value : ClientId) : string = Guid.toStringN %value
