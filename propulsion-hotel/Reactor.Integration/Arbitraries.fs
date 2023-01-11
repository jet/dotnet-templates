[<AutoOpen>]
module Reactor.Integration.Arbitraries

open FsCheck.FSharp
open FSharp.UMX

type GuidStringN<[<Measure>]'m> = GuidStringN of string<'m> with static member op_Explicit(GuidStringN x) = x

let genDefault<'t> = ArbMap.defaults |> ArbMap.generate<'t>

type Custom =

    static member GuidStringN() = genDefault |> Gen.map (Domain.Guid.toString >> GuidStringN) |> Arb.fromGen

[<assembly: FsCheck.Xunit.Properties( Arbitrary = [| typeof<Custom> |] )>] do()
