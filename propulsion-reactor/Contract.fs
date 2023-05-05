module ReactorTemplate.Contract

//#if (!blank)
/// A single Item in the list
type ItemInfo = { id: int; order: int; title: string; completed: bool }

type SummaryInfo = { items: ItemInfo[] }

let render (item: Todo.Events.ItemData): ItemInfo =
    {   id = item.id
        order = item.order
        title = item.title
        completed = item.completed }
let ofState (state: Todo.Fold.State): SummaryInfo =
    { items = [| for x in state.items -> render x |]}

//#endif
//#if kafka
#if blank
module Input =

    let [<Literal>] Category = "CategoryName"
    let [<return: Struct>] (|StreamName|_|) = function FsCodec.StreamName.CategoryAndId (Category, ClientId.Parse clientId) -> ValueSome clientId | _ -> ValueNone
    
    type Value = { field: int }
    type Event =
        | EventA of Value
        | EventB of Value
        interface TypeShape.UnionContract.IUnionContract
    let private dec = Streams.Codec.genWithIndex<Event>

    let [<return: Struct>] (|Parse|_|) = function
        | struct (StreamName clientId, _) & Streams.Decode dec events -> ValueSome struct (clientId, events)
        | _ -> ValueNone

type Data = { value: int }
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
let codec = Streams.Codec.gen<SummaryEvent>
let encode summary = codec.Encode((), summary)
//#endif
