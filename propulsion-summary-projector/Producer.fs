module ProjectorTemplate.Producer

open System.Runtime.Serialization

module Contract =

    /// A single Item in the Todo List
    type ItemInfo = { id: int; order: int; title: string; completed: bool }

    /// All data summarized for Summary Event Stream
    type SummaryInfo = { items : ItemInfo[] }

    /// Events we emit to third parties (kept here for ease of comparison, can be moved elsewhere in a larger app)
    type SummaryEvent =
        | [<DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<SummaryEvent>()

    let render (item: Todo.Events.ItemData) : ItemInfo =
        {   id = item.id
            order = item.order
            title = item.title
            completed = item.completed }
    let ofState (state : Todo.Folds.State) : SummaryEvent =
        Summary { items = [| for x in state.items -> render x |]}

let (|ClientId|) (value : string) = ClientId.parse value

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
        return version'
    | _ ->
        return span.index + span.events.LongLength }