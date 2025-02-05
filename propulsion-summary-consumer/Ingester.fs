/// Follows a feed of updates, holding the most recently observed one; each update received is intended to completely supersede all previous updates
/// Due to this, we should ensure that writes only happen where the update is not redundant and/or a replay of a previous message
module ConsumerTemplate.Ingester

/// Defines the contract we share with the proReactor --'s published feed
module Contract =

    let [<Literal>] Category = "TodoSummary"
    let decodeId = FsCodec.StreamId.dec ClientId.parse
    let tryDecode = FsCodec.StreamName.tryFind Category >> ValueOption.map decodeId

    /// A single Item in the list
    type ItemInfo = { id: int; order: int; title: string; completed: bool }

    /// All data summarized for Summary Event Stream
    type SummaryInfo = { items: ItemInfo[] }

    type Message =
        | [<System.Runtime.Serialization.DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
        interface TypeShape.UnionContract.IUnionContract
        
    // We also want the index (which is the Version of the Summary) whenever we're handling an event
    type VersionAndMessage = int64*Message
    let private dec: Propulsion.Sinks.Codec<VersionAndMessage> = Streams.Codec.genWithIndex<Message>
    let [<return: Struct>] (|For|_|) = tryDecode
    let [<return: Struct>] (|Parse|_|) = function
        | struct (For clientId, _) & Streams.DecodeNewest dec (version, update) -> ValueSome struct (clientId, version, update)
        | _ -> ValueNone

[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used: int * unused: int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count: int
    /// Handler determined the events were not relevant to its duties and performed no decoding or processing
    | NotApplicable of count: int

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, na, skipped = 0, 0, 0
    override _.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" Used {ok} Skipped {skipped} N/A {na}", ok, skipped, na)
            ok <- 0; skipped <- 0l; na <- 0

    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")
        
/// Map from external contract to internal contract defined by the aggregate
let map: Contract.Message -> TodoSummary.Events.SummaryData = function
    | Contract.Summary x ->
        { items =
            [| for x in x.items ->
                { id = x.id; order = x.order; title = x.title; completed = x.completed } |]}

/// Ingest queued events per client - each time we handle all the incoming updates for a given stream as a single act
let ingest (service: TodoSummary.Service) stream (events: Propulsion.Sinks.Event[]) = async {
    match struct (stream, events) with
    | Contract.Parse (clientId, version, update) ->
        match! service.TryIngest(clientId, version, map update) with
        | true -> return Outcome.Ok (1, events.Length - 1), Propulsion.Sinks.Events.next events
        | false -> return Outcome.Skipped events.Length, Propulsion.Sinks.Events.next events
    | _ -> return Outcome.NotApplicable events.Length, Propulsion.Sinks.Events.next events }
