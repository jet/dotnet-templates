namespace TodoBackendTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// CartId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toStringN (value : ClientId) : string = Guid.toStringN %value