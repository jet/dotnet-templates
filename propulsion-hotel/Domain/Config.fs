module Domain.Config

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Store =
    | Memory of Equinox.MemoryStore.VolatileStore<obj>
    | Dynamo of Equinox.DynamoStore.DynamoStoreContext * Equinox.Core.ICache

let log = Serilog.Log.ForContext("isMetric", true)
let resolve cat = Equinox.Decider.resolve log cat

module EventCodec =

    open FsCodec.SystemTextJson

    let private defaultOptions = Options.Create()
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        Codec.Create<'t>(options = defaultOptions)

module Memory =

    let create _codec initial fold store : Equinox.Category<_, _, _> =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial)

module Dynamo =

    open Equinox.DynamoStore

    let private create codec initial fold accessStrategy (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        DynamoStoreCategory(context, FsCodec.Deflate.EncodeUncompressed codec, fold, initial, cacheStrategy, accessStrategy)

    let createUnoptimized codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        create codec initial fold accessStrategy (context, cache)

    let createSnapshotted codec initial fold (isOrigin, toSnapshot) (context, cache) =
        let accessStrategy = AccessStrategy.Snapshot (isOrigin, toSnapshot)
        create codec initial fold accessStrategy (context, cache)

    // let createRollingState codec initial fold toSnapshot (context, cache) =
    //     let accessStrategy = AccessStrategy.RollingState toSnapshot
    //     create codec initial fold accessStrategy (context, cache)
