module Patterns.Domain.Tests.ExactlyOnceIngesterTests

open Patterns.Domain
open FsCheck
open Swensen.Unquote

let linger, maxPickTicketsPerBatch = System.TimeSpan.FromMilliseconds 1., 5

let createSut store =
    // While we use ~ 200ms when hitting Cosmos, there's no value in doing so in the context of these property based tests
    ItemIngester.MemoryStore.Create(store, linger=linger, maxItemsPerEpoch=maxPickTicketsPerBatch)

let [<DomainProperty>] properties shouldUseSameSut (initialEpochId, Ids initialItems) (Ids items) =
    let store = Equinox.MemoryStore.VolatileStore()
    
    // https://blog.ploeh.dk/2016/05/17/tie-fighter-fscheck-properties/
    Gen.choose (0, 3) |> Arb.fromGen |> Prop.forAll <| fun gap ->
        
    let mutable nextEpochId = initialEpochId
    for x in 1 .. gap do nextEpochId <- ItemEpochId.next nextEpochId
        
    Async.RunSynchronously <| async {
        // Initialize with some items
        let initialSut = createSut store
        let! initialResult = initialSut.IngestMany(initialEpochId, initialItems)
        let initialExpected = initialItems
        test <@ set initialExpected = set initialResult @>

        // Add some extra
        let sut = if shouldUseSameSut then initialSut else createSut store
        let! result = sut.IngestMany(nextEpochId, items)
        let expected = items |> Seq.except initialExpected |> Seq.distinct
        return set expected = set result
    }
