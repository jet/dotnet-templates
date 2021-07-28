module Patterns.Domain.Tests.ExactlyOnceIngesterTests

open Patterns.Domain
open Patterns.Domain.ItemIngester
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote

let linger, maxPickTicketsPerBatch = System.TimeSpan.FromMilliseconds 1., 5

let createSut store =
    // While we use ~ 200ms when hitting Cosmos, there's no value in doing so in the context of these property based tests
    MemoryStore.Create(store, linger=linger, maxItemsPerEpoch=maxPickTicketsPerBatch)

let [<Property>] properties shouldUseSameSut (initialEpochId, Ids initialItems) (Ids items) =
    let store = Equinox.MemoryStore.VolatileStore()
    Async.RunSynchronously <| async {
        // Initialize with some items
        let initialSut = createSut store
        let! initialResult = initialSut.IngestMany(initialEpochId, initialItems)
        let initialExpected = initialItems |> Array.ofSeq
        test <@ set initialExpected = set initialResult @>

        // Add some extra
        let sut = if shouldUseSameSut then initialSut else createSut store
        let mutable nextEpochId = initialEpochId
        for x in 0 .. (Gen.choose (0, 3)) do
            nextEpochId <- ItemEpochId.next nextEpochId
        let! result = sut.IngestMany(nextEpochId, items)
        let expected = items |> Seq.except initialExpected |> Seq.distinct
        test <@ set expected = set result @>
    }
//
//let [<AutoData>] ``lookBack is limited`` (Id trancheId) genItem =
//    let store = Equinox.MemoryStore.VolatileStore()
//    Async.RunSynchronously <| async {
//        // Initialize with more items than the lookBack accommodates
//        let initialSut = createSut store trancheId
//        let itemCount =
//            // Fill up lookBackLimit batches, and another one as batch 0 that we will not look include in the load
//            (lookBackLimit+1) * maxPickTicketsPerBatch
//            // Add one more so we end up with an active batchId = lookBackLimit
//            + 1
//        let items = Array.init itemCount (fun _ -> genItem () )
//        test <@ Array.distinct items = items @>
//        let batch0 = Array.take maxPickTicketsPerBatch items
//        let batchesInLookBack = Array.skip maxPickTicketsPerBatch items
//        let! b0Added = initialSut.IngestMany batch0
//        let b0Added = Array.ofSeq b0Added
//        test <@ maxPickTicketsPerBatch = Array.length b0Added @>
//        let! batchesInLookBackAdded = initialSut.IngestMany batchesInLookBack
//        test <@ itemCount = Set.count (set b0Added + set batchesInLookBackAdded) @>
//
//        // Now try to add the same items - the first batch worth should not be deduplicated
//        let sut = createSut store trancheId
//        let! result = sut.IngestMany items
//        let result = Array.ofSeq result
//        test <@ itemCount = itemCount
//                && result.Length = maxPickTicketsPerBatch
//                && set result = set b0Added @>
//    }
