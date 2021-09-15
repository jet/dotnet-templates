module ReactorTemplate.Contract

//#if (!blank)
/// A single Item in the list
type ItemInfo = { id: int; order: int; title: string; completed: bool }

type SummaryInfo = { items : ItemInfo[] }

let render (item: Todo.Events.ItemData) : ItemInfo =
    {   id = item.id
        order = item.order
        title = item.title
        completed = item.completed }
let ofState (state : Todo.Fold.State) : SummaryInfo =
    { items = [| for x in state.items -> render x |]}

//#endif
//#if kafka
#if blank
module Input =

    let [<Literal>] Category = "CategoryName"
    type Value = { field : int }
    type Event =
        | EventA of Value
        | EventB of Value
        interface TypeShape.UnionContract.IUnionContract
    let codec =
        let up (evt : FsCodec.ITimelineEvent<_>, e : Event) =
            evt.Index, e
        let down (_, e) = e, None, None
        FsCodec.NewtonsoftJson.Codec.Create(up, down)

    let (|Decode|) (stream, span : Propulsion.Streams.StreamSpan<_>) =
        span.events |> Array.choose (EventCodec.tryDecode codec stream)
    let (|StreamName|_|) = function FsCodec.StreamName.CategoryAndId (Category, ClientId.Parse clientId) -> Some clientId | _ -> None
    let (|Parse|_|) = function
        | (StreamName clientId, _) & (Decode events) -> Some (clientId, events)
        | _ -> None

type Data = { value : int }
type SummaryEvent =
    | EventA of Data
    | EventB of Data
    interface TypeShape.UnionContract.IUnionContract
#else
/// Events we emit to third parties (kept here for ease of comparison, can be moved elsewhere in a larger app)
type SummaryEvent =
    | [<System.Runtime.Serialization.DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
    interface TypeShape.UnionContract.IUnionContract
#endif
let codec = FsCodec.NewtonsoftJson.Codec.Create<SummaryEvent>()
let encode summary = codec.Encode(None, summary)
//#endif
