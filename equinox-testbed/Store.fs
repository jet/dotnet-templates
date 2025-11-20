module TestbedTemplate.Store

module Metrics =
    
    let [<Literal>] PropertyTag = "isMetric"
    let log = Serilog.Log.ForContext(PropertyTag, true)
    /// Allow logging to filter out emission of log messages whose information is also surfaced as metrics
    let logEventIsMetric e = Serilog.Filters.Matching.WithProperty(PropertyTag).Invoke e

let createDecider cat = Equinox.Decider.forStream Metrics.log cat

module Codec =

    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>() // options = Options.Default
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() |> FsCodec.SystemTextJson.Encoder.Uncompressed // options = Options.Default

module Memory =

    let create name _codec initial fold store: Equinox.Category<_, _, _> =
        // While the actual prod codec can be used, the Box codec allows one to stub out the decoding on the basis that
        // nothing will be proved beyond what a complete roundtripping test per `module Aggregate` would already cover
        Equinox.MemoryStore.MemoryStoreCategory(store, name, FsCodec.Box.Codec.Create(), fold, initial)

module Cosmos =

    let create name codec initial fold accessStrategy cacheStrategy context =
        Equinox.CosmosStore.CosmosStoreCategory(context, name, FsCodec.SystemTextJson.Encoder.Uncompressed codec, fold, initial, accessStrategy, cacheStrategy)

module Esdb =

    let create name codec initial fold accessStrategy cacheStrategy context =
        Equinox.EventStoreDb.EventStoreCategory(context, name, codec, fold, initial, accessStrategy, cacheStrategy)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Config =
//#if (memoryStore || (!cosmos && !eventStore))
    | Memory of Equinox.MemoryStore.VolatileStore<obj>
//#endif
//#if cosmos
    | Cosmos of Equinox.CosmosStore.CosmosStoreContext * Equinox.CachingStrategy * unfolds: bool
//#endif
//#if eventStore
    | Esdb of Equinox.EventStoreDb.EventStoreContext * Equinox.CachingStrategy * unfolds: bool
//#endif
