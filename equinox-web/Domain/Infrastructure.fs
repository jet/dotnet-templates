namespace TodoBackendTemplate

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module Guid =
    let inline tryParse (x: string) = match Guid.TryParse x with true, x -> Some x | false, _ -> None
    let inline toStringN (x: Guid) = x.ToString "N"

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let ofGuid (value: Guid): ClientId = %value
    let toString (value: ClientId): string = Guid.toStringN %value
