[<AutoOpen>]
module Patterns.Domain.Tests.Infrastructure

open FSharp.UMX
open System

(* Generic FsCheck helpers *)

let (|Id|) (x : Guid) = x.ToString "N" |> UMX.tag
let (|Ids|) (xs : Guid[]) = xs |> Array.map (|Id|)
