/// Emits summaries based on an aggregate's folded state as prompted by observing events indicative of state changes in the change feed
module ProjectorTemplate.Producer

module Contract =

    /// A single Item in the Todo List
    type ItemInfo = { id: int; order: int; title: string; completed: bool }

    /// All data summarized for Summary Event Stream
    type SummaryInfo = { items : ItemInfo[] }

    /// Events we emit to third parties (kept here for ease of comparison, can be moved elsewhere in a larger app)
    type SummaryEvent =
        | [<System.Runtime.Serialization.DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<SummaryEvent>()

    let render (item: Todo.Events.ItemData) : ItemInfo =
        {   id = item.id
            order = item.order
            title = item.title
            completed = item.completed }
    let ofState (state : Todo.Folds.State) : SummaryEvent =
        Summary { items = [| for x in state.items -> render x |]}

let (|ClientId|) = ClientId.parse

let (|Decode|) (codec : FsCodec.IUnionEncoder<_,_>) stream (span : Propulsion.Streams.StreamSpan<_>) =
    span.events |> Seq.choose (StreamCodec.tryDecodeSpan codec Serilog.Log.Logger stream)

let handleAccumulatedEvents
        (service : Todo.Service)
        (produce : Propulsion.Codec.NewtonsoftJson.RenderedSummary -> Async<_>)
        (stream, span : Propulsion.Streams.StreamSpan<_>) : Async<int64> = async {
    match stream, span with
    | Category (Todo.categoryId, ClientId clientId), (Decode Todo.Events.codec stream events)
            when events |> Seq.exists Todo.Folds.impliesStateChange ->
        let! version', summary = service.QueryWithVersion(clientId, Contract.ofState)
        let rendered : Propulsion.Codec.NewtonsoftJson.RenderedSummary = summary |> StreamCodec.encodeSummary Contract.codec stream version'
        let! _ = produce rendered
        // We need to yield the next write position, which will be after the version we've just generated the summary based on
        return version'+1L
    | _ ->
        // If we're ignoring the events, we mark the next write position to be one beyond the last one offered
        let x = Array.last span.events in return x.Index+1L }