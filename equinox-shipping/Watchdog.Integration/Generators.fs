[<AutoOpen>]
module Shipping.Watchdog.Integration.Generators

open FsCheck
open FsCheck.FSharp
open FsCheck.Xunit
open FSharp.UMX

type GuidStringN<[<Measure>]'m> = GuidStringN of string<'m> with static member op_Explicit(GuidStringN x) = x

let (|Ids|) xs = Array.map (function GuidStringN x -> x) xs
let (|IdsAtLeastOne|) (x, xs) = Array.append (Array.singleton x) (Array.map (function GuidStringN x -> x) xs)

let genDefault<'t> = ArbMap.defaults |> ArbMap.generate<'t>
type Custom =

    static member GuidStringN() = genDefault |> Gen.map (Shipping.Domain.Guid.toStringN >> GuidStringN) |> Arb.fromGen

[<assembly: Properties( Arbitrary = [| typeof<Custom> |] )>] do()
