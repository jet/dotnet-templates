module AllTemplate.Contract

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