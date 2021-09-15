module Patterns.Domain.Tests.ExactlyOnceIngesterTests

open Patterns.Domain
open FsCheck
open Swensen.Unquote

let linger, maxItemsPerEpoch = System.TimeSpan.FromMilliseconds 1., 5

let createSut store =
    // While we use ~ 200ms when hitting Cosmos, there's no value in doing so in the context of these property based tests
    ListIngester.Config.Memory.Create(store, linger=linger, maxItemsPerEpoch=maxItemsPerEpoch)

type Gap = Gap of int
type GapGen =
    static member Gap = Gen.choose (0, 3) |> Arb.fromGen
type IngesterProperty() = inherit FsCheck.Xunit.PropertyAttribute(Arbitrary=[|typeof<GapGen>|])

let [<IngesterProperty>] properties shouldUseSameSut (Gap gap) (initialEpochId, Ids initialItems) (Ids items) =
    let store = Equinox.MemoryStore.VolatileStore()

    let mutable nextEpochId = initialEpochId
    for x in 1 .. gap do nextEpochId <- ListEpochId.next nextEpochId

    Async.RunSynchronously <| async {
        // Initialize with some items
        let initialSut = createSut store
        let! initialResult = initialSut.IngestMany(initialEpochId, initialItems)
        let initialExpected = initialItems
        test <@ set initialExpected = set initialResult @>

        // Add more, can be overlapping, adjacent
        let sut = if shouldUseSameSut then initialSut else createSut store
        let! result = sut.IngestMany(nextEpochId, items)
        let expected = items |> Seq.except initialExpected

        return set expected = set result
            |> Prop.collect gap
            |> Prop.classify (initialItems.Length >= maxItemsPerEpoch) "fill"
            |> Prop.classify (gap = 0) "overlapping"
            |> Prop.classify (gap = 1) "adjacent"
            |> Prop.classify (items.Length >= maxItemsPerEpoch) "fillNext"
    }
