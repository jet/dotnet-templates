[<AutoOpen>]
module Shipping.Domain.Tests.Fixtures

(* Generic FsCheck helpers *)

let (|Id|) (x : System.Guid) = x.ToString "N" |> FSharp.UMX.UMX.tag
let (|Ids|) (xs : System.Guid[]) = xs |> Array.map (|Id|)
let (|IdsAtLeastOne|) (Ids xs, Id x) = [| yield x; yield! xs |]
let (|AtLeastOne|) (x, xs) = x::xs
