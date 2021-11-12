[<AutoOpen>]
module Shipping.Watchdog.Integration.Infrastructure

open FsCheck
open FsCheck.FSharp
open FsCheck.Xunit
open FSharp.UMX
open System

module Async =

    /// Wraps a computation, cancelling (and triggering a timeout exception) if it doesn't complete within the specified timeout
    let timeoutAfter (timeout : TimeSpan) (c : Async<'a>) = async {
        let! r = Async.StartChild(c, int timeout.TotalMilliseconds)
        return! r }

type GuidStringN<[<Measure>]'m> = GuidStringN of string<'m> with static member op_Explicit(GuidStringN x) = x

let (|Ids|) xs = Array.map (function GuidStringN x -> x) xs
let (|IdsAtLeastOne|) (x, xs) = Array.append (Array.singleton x) (Array.map (function GuidStringN x -> x) xs)

let genDefault<'t> = ArbMap.defaults |> ArbMap.generate<'t>
type Custom =

    static member GuidStringN() = genDefault |> Gen.map (Shipping.Domain.Guid.toStringN >> GuidStringN) |> Arb.fromGen

[<assembly: Properties( Arbitrary = [| typeof<Custom> |] )>] do()
