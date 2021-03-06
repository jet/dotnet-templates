[<AutoOpen>]
module Patterns.Domain.Tests.Infrastructure

open FSharp.UMX
open System
open FsCheck
open Patterns.Domain

(* Generic FsCheck helpers *)

let (|Id|) (x : Guid) = x.ToString "N" |> UMX.tag
let inline mkId () = Guid.NewGuid() |> (|Id|)
let (|Ids|) (xs : Guid[]) = xs |> Array.map (|Id|)

type DomainArbs() =

    static member Item : Arbitrary<ItemEpoch.Events.Item> = Arb.fromGen <| gen {
        let! r = Arb.Default.Derive() |> Arb.toGen
        let id = mkId () // TODO why doesnt `let (Id id) = Arb.generate` generate fresh every time?
        return { r with id = id }
    }

type DomainProperty() = inherit FsCheck.Xunit.PropertyAttribute(Arbitrary=[|typeof<DomainArbs>|], QuietOnSuccess=true)

/// Inspired by AutoFixture.XUnit's AutoDataAttribute - generating test data without the full Property Based Tests experience
/// By using this instead of Property, the developer has
/// a) asserted by using this property instead of [<DomainProperty>]
/// b) indirectly validated by running the tests frequently locally in DEBUG mode
/// that running the test multiple times is not a useful thing to do
#if !DEBUG
type AutoDataAttribute() = inherit FsCheck.Xunit.PropertyAttribute(Arbitrary=[|typeof<DomainArbs>|], MaxTest=1, QuietOnSuccess=true)
#else
type AutoDataAttribute() = inherit FsCheck.Xunit.PropertyAttribute(Arbitrary=[|typeof<DomainArbs>|], MaxTest=5, QuietOnSuccess=true)
#endif