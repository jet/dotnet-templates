/// Follows a feed of updates, holding the most recently observed one; each update received is intended to completely supersede all previous updates
/// Due to this, we should ensure that writes only happen where the update is not redundant and/or a replay of a previous message
module ConsumerTemplate.Ingester

open Propulsion.Internal

/// Defines the contract we share with the proReactor --'s published feed
module Contract =

    let [<Literal>] Category = "TodoSummary"

    /// A single Item in the list
    type ItemInfo = { id : int; order : int; title : string; completed : bool }

    /// All data summarized for Summary Event Stream
    type SummaryInfo = { items : ItemInfo[] }

    type Message =
        | [<System.Runtime.Serialization.DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
        interface TypeShape.UnionContract.IUnionContract
    type VersionAndMessage = int64*Message
    // We also want the index (which is the Version of the Summary) whenever we're handling an event
    let private codec : FsCodec.IEventCodec<VersionAndMessage, _, _> = Config.EventCodec.withIndex<Message>
    let [<return: Struct>] (|DecodeNewest|_|) (stream, span : Propulsion.Streams.StreamSpan<_>) : VersionAndMessage voption =
        span |> Seq.rev |> Seq.tryPickV (EventCodec.tryDecode codec stream)
    let [<return: Struct>] (|StreamName|_|) = function
        | FsCodec.StreamName.CategoryAndId (Category, ClientId.Parse clientId) -> ValueSome clientId
        | _ -> ValueNone
    let [<return: Struct>] (|MatchNewest|_|) = function
        | (StreamName clientId, _) & DecodeNewest (version, update) -> ValueSome struct (clientId, version, update)
        | _ -> ValueNone

[<RequireQualifiedAccess>]
type Outcome =
    /// Handler processed the span, with counts of used vs unused known event types
    | Ok of used : int * unused : int
    /// Handler processed the span, but idempotency checks resulted in no writes being applied; includes count of decoded events
    | Skipped of count : int
    /// Handler determined the events were not relevant to its duties and performed no decoding or processing
    | NotApplicable of count : int

/// Gathers stats based on the Outcome of each Span as it's processed, for periodic emission via DumpStats()
type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Stats<Outcome>(log, statsInterval, stateInterval)

    let mutable ok, na, skipped = 0, 0, 0

    override _.HandleOk res = res |> function
        | Outcome.Ok (used, unused) -> ok <- ok + used; skipped <- skipped + unused
        | Outcome.Skipped count -> skipped <- skipped + count
        | Outcome.NotApplicable count -> na <- na + count
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

    override _.DumpStats() =
        base.DumpStats()
        if ok <> 0 || skipped <> 0 || na <> 0 then
            log.Information(" Used {ok} Skipped {skipped} N/A {na}", ok, skipped, na)
            ok <- 0; skipped <- 0l; na <- 0

/// Map from external contract to internal contract defined by the aggregate
let map : Contract.Message -> TodoSummary.Events.SummaryData = function
    | Contract.Summary x ->
        { items =
            [| for x in x.items ->
                { id = x.id; order = x.order; title = x.title; completed = x.completed } |]}

/// Ingest queued events per client - each time we handle all the incoming updates for a given stream as a single act
let ingest (service : TodoSummary.Service) stream (span : Propulsion.Streams.StreamSpan<_>) ct = Async.startImmediateAsTask ct <| async {
    match stream, span with
    | Contract.MatchNewest (clientId, version, update) ->
        match! service.TryIngest(clientId, version, map update) with
        | true -> return struct (Propulsion.Streams.SpanResult.AllProcessed, Outcome.Ok (1, span.Length - 1))
        | false -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.Skipped span.Length
    | _ -> return Propulsion.Streams.SpanResult.AllProcessed, Outcome.NotApplicable span.Length }
