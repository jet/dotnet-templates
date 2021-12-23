module Patterns.Domain.Tests.ExactlyOnceIngesterTests

open Patterns.Domain
open FsCheck
open FsCheck.FSharp
open FsCheck.Xunit
open FSharp.UMX
open Swensen.Unquote

let linger, maxItemsPerEpoch = System.TimeSpan.FromMilliseconds 1., 5

let createSut =
    // While we use ~ 200ms when hitting Cosmos, there's no value in doing so in the context of these property based tests
    ListIngester.Config.create_ linger maxItemsPerEpoch

type GuidStringN<[<Measure>]'m> = GuidStringN of string<'m> with static member op_Explicit(GuidStringN x) = x
let (|Ids|) = Array.map (function GuidStringN x -> x)

type Gap = Gap of int with static member op_Explicit(Gap i) = i

let genDefault<'t> = ArbMap.defaults |> ArbMap.generate<'t>
type Custom =

    static member GuidStringN() = genDefault |> Gen.map (Guid.toStringN >> GuidStringN) |> Arb.fromGen
    static member Gap() = Gen.choose (0, 3) |> Gen.map Gap |> Arb.fromGen

[<assembly: Properties( Arbitrary = [| typeof<Custom> |] )>] do()

let [<Property>] properties shouldUseSameSut (Gap gap) (initialEpochId, NonEmptyArray (Ids initialItems)) (NonEmptyArray (Ids items)) = async {

    let store = Equinox.MemoryStore.VolatileStore() |> Config.Store.Memory

    let mutable nextEpochId = initialEpochId
    for _ in 1 .. gap do nextEpochId <- ExactlyOnceIngester.Internal.next nextEpochId

    // Initialize with some items
    let initialSut = createSut store
    let! initialResult = initialSut.IngestItems(initialEpochId, initialItems)
    let initialExpected = initialItems
    test <@ set initialExpected = set initialResult @>

    // Add more, can be overlapping, adjacent
    let sut = if shouldUseSameSut then initialSut else createSut store
    let! result = sut.IngestItems(nextEpochId, items)
    let expected = items |> Seq.except initialExpected

    test <@ set expected = set result @> }
    // TODO port to FsCheck v3
    // |> Prop.collect gap
    // |> Prop.classify (initialItems.Length >= maxItemsPerEpoch) "fill"
    // |> Prop.classify (gap = 0) "overlapping"
    // |> Prop.classify (gap = 1) "adjacent"
    // |> Prop.classify (items.Length >= maxItemsPerEpoch) "fillNext"
