module ReactorTemplate.Contract

/// A single Item in the Todo List
type ItemInfo = { id: int; order: int; title: string; completed: bool }

type SummaryInfo = { items : ItemInfo[] }

let render (item: Todo.Events.ItemData) : ItemInfo =
    {   id = item.id
        order = item.order
        title = item.title
        completed = item.completed }
let ofState (state : Todo.Fold.State) : SummaryInfo =
    { items = [| for x in state.items -> render x |]}

//#if kafka
/// Events we emit to third parties (kept here for ease of comparison, can be moved elsewhere in a larger app)
type SummaryEvent =
    | [<System.Runtime.Serialization.DataMember(Name="TodoUpdateV1")>] Summary of SummaryInfo
    interface TypeShape.UnionContract.IUnionContract
let codec = FsCodec.NewtonsoftJson.Codec.Create<SummaryEvent>()
let encode summary = codec.Encode(None, summary)
//#endif