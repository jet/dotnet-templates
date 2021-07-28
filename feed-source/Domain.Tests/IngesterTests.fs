module FeedSourceTemplate.Domain.Tests.IngesterTests

open FeedSourceTemplate.Domain
open FsCheck.Xunit
open FSharp.UMX
open Swensen.Unquote

let linger, lookBackLimit, maxPickTicketsPerBatch = System.TimeSpan.FromMilliseconds 1., 2, 5

let createSut store trancheId =
    // While we use ~ 200ms when hitting Cosmos, there's no value in doing so in the context of these property based tests
    let service = TicketsIngester.MemoryStore.Create(store, linger=linger, maxItemsPerEpoch=maxPickTicketsPerBatch, lookBackLimit=lookBackLimit)
    service.ForFc trancheId

let [<Property>] properties shouldInitialize shouldUseSameSut (Id trancheId) initialItems items =
    let store = Equinox.MemoryStore.VolatileStore()
    Async.RunSynchronously <| async {
        // Initialize with some items
        let initialSut = createSut store trancheId
        if shouldInitialize then do! initialSut.Initialize()
        let! initialResult = initialSut.IngestMany(initialItems)
        let initialExpected = initialItems |> Seq.map TicketsEpoch.itemId |> Array.ofSeq
        test <@ set initialExpected = set initialResult @>

        // Add some extra
        let sut = if shouldUseSameSut then initialSut else createSut store trancheId
        if shouldInitialize then do! sut.Initialize()
        let! result = sut.IngestMany items
        let expected = items |> Seq.map TicketsEpoch.itemId |> Seq.except initialExpected |> Seq.distinct
        test <@ set expected = set result @>

        // Add the same stuff for a different tranche; the data should be completely independent from an ingestion perspective
        let differentTranche = %(sprintf "%s2" %trancheId)
        let differentSutSameStore = createSut store differentTranche
        let! independentResult = differentSutSameStore.IngestMany(Array.append initialItems items)
        test <@ set initialResult + set result = set independentResult @>
    }

let [<AutoData>] ``lookBack is limited`` (Id trancheId) genItem =
    let store = Equinox.MemoryStore.VolatileStore()
    Async.RunSynchronously <| async {
        // Initialize with more items than the lookBack accommodates
        let initialSut = createSut store trancheId
        let itemCount =
            // Fill up lookBackLimit batches, and another one as batch 0 that we will not look include in the load
            (lookBackLimit+1) * maxPickTicketsPerBatch
            // Add one more so we end up with an active batchId = lookBackLimit
            + 1
        let items = Array.init itemCount (fun _ -> genItem () )
        test <@ Array.distinct items = items @>
        let batch0 = Array.take maxPickTicketsPerBatch items
        let batchesInLookBack = Array.skip maxPickTicketsPerBatch items
        let! b0Added = initialSut.IngestMany batch0
        let b0Added = Array.ofSeq b0Added
        test <@ maxPickTicketsPerBatch = Array.length b0Added @>
        let! batchesInLookBackAdded = initialSut.IngestMany batchesInLookBack
        test <@ itemCount = Set.count (set b0Added + set batchesInLookBackAdded) @>

        // Now try to add the same items - the first batch worth should not be deduplicated
        let sut = createSut store trancheId
        let! result = sut.IngestMany items
        let result = Array.ofSeq result
        test <@ itemCount = itemCount
                && result.Length = maxPickTicketsPerBatch
                && set result = set b0Added @>
    }
