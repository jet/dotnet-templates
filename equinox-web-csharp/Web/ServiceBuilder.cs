namespace TodoBackendTemplate.Web;

/// Binds a storage independent Service's Handler's `resolve` function to a given Stream Policy using the StreamResolver
internal class ServiceBuilder
{
    readonly EquinoxContext _context;
    readonly Serilog.ILogger _handlerLog;

    public ServiceBuilder(EquinoxContext context, Serilog.ILogger handlerLog)
    {
        _context = context;
        _handlerLog = handlerLog;
    }

#if todos
    public Todo.Service CreateTodoService()
    {
        var resolve =
            _context.Resolve(
                Todo.Event.Category,
                _handlerLog,
                EquinoxCodec.Create<Todo.IEvent>(Todo.Event.Encode, Todo.Event.TryDecode),
                Todo.State.Fold,
                Todo.State.Initial,
                Todo.State.IsOrigin,
                Todo.State.Snapshot);
        return new(id => resolve(Todo.Event.StreamId(id)));
    }

#endif
#if aggregate
    public Aggregate.Service CreateAggregateService()
    {
        var resolve =
            _context.Resolve(
                Aggregate.Event.Category,
                _handlerLog,
                EquinoxCodec.Create<Aggregate.IEvent>(Aggregate.Event.Encode, Aggregate.Event.TryDecode),
                Aggregate.State.Fold,
                Aggregate.State.Initial,
                Aggregate.State.IsOrigin,
                Aggregate.State.Snapshot);
        return new Aggregate.Service(id => resolve(Aggregate.Event.StreamId(id)));
    }
#endif
#if (!aggregate && !todos)
//    public Thing.Service CreateThingService()
//    {
//        var resolve =
//            _context.Resolve(
//                Thing.Event.Category,
//                _handlerLog,
//                EquinoxCodec.Create<Thing.IEvent>(Thing.Event.Encode, Thing.Event.TryDecode),
//                Thing.Fold.Fold,
//                Thing.Fold.Initial,
//                Thing.Fold.IsOrigin,
//                Thing.Fold.Snapshot);
//        return new Thing.Service(id => resolve(Thing.Event.StreamId(id)));
//    }
#endif
}
