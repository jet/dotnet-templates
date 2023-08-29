[<AutoOpen>]
module Infrastructure

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings
open System

module Guid =

    let inline toStringN (x: Guid) = x.ToString "N"

module TimeSpan =

    let seconds value = TimeSpan.FromSeconds value

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toString (value: ClientId): string = Guid.toStringN %value
    let parse (value: string): ClientId = let raw = Guid.Parse value in % raw
    let (|Parse|) = parse

type Equinox.Decider<'e, 's> with

    member x.TransactWithPostVersion(decide: 's -> 'r * 'e[]) =
        x.TransactEx((fun (c: Equinox.ISyncContext<_>) -> decide c.State),
                     (fun res (c: Equinox.ISyncContext<_>) -> res, c.Version))

type DataMemberAttribute = System.Runtime.Serialization.DataMemberAttribute
