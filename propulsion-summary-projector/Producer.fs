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

let generate stream version summary =
    let event = Contract.codec.Encode summary
    Propulsion.Codec.NewtonsoftJson.RenderedSummary.ofStreamEvent stream version event