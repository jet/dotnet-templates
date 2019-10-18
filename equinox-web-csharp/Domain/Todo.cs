using Equinox;
using Equinox.Core;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using OneOf;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public static class Todo
    {
        /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
        public abstract class Event
        : OneOfBase<Event.Added, Event.Updated, Event.Deleted, Event.Cleared, Event.Compacted>
        {
            /// Information we retain per Todo List entry
            public class ItemData
            {
                public int Id { get; set; }
                public int Order { get; set; }
                public string Title { get; set; }
                public bool Completed { get; set; }
            }

            public abstract class ItemEvent : Event
            {
                protected ItemEvent() => Data = new ItemData();
                public ItemData Data { get; set; }
            }

            public class Added : ItemEvent
            {
            }

            public class Updated : ItemEvent
            {
            }

            public class Deleted : Event
            {
                public int Id { get; set; }
            }

            public class Cleared : Event
            {
                public int NextId { get; set; }
            }

            public class Compacted : Event
            {
                public int NextId { get; set; }
                public ItemData[] Items { get; set; }
            }

            static readonly JsonNetUtf8Codec Codec = new JsonNetUtf8Codec(new JsonSerializerSettings());

            public static Event TryDecode(string et, byte[] json)
            {
                switch (et)
                {
                    case nameof(Added): return Codec.Decode<Added>(json);
                    case nameof(Updated): return Codec.Decode<Updated>(json);
                    case nameof(Deleted): return Codec.Decode<Deleted>(json);
                    case nameof(Cleared): return Codec.Decode<Cleared>(json);
                    case nameof(Compacted): return Codec.Decode<Compacted>(json);
                    default: return null;
                }
            }

            public static Tuple<string, byte[]> Encode(Event e) => Tuple.Create(e.GetType().Name, Codec.Encode(e));
        }

        /// Present state of the Todo List as inferred from the Events we've seen to date
        // NB the value of the state is only ever manipulated in a cloned copy within Fold()
        // This is critical for caching and/or concurrent transactions to work correctly
        // In the F# impl, this is achieved by virtue of the fact that records and [F#] lists represent
        // persistent data structures https://en.wikipedia.org/wiki/Persistent_data_structure
        public class State
        {
            public int NextId { get; }
            public Event.ItemData[] Items { get; }

            internal State(int nextId, Event.ItemData[] items)
            {
                NextId = nextId;
                Items = items;
            }

            public static State Initial = new State(0, new Event.ItemData[0]);

            /// Folds a set of events from the store into a given `state`
            public static State Fold(State origin, IEnumerable<Event> xs)
            {
                var nextId = origin.NextId;
                var items = origin.Items.ToList();
                foreach (var x in xs)
                    x.Switch(
                        (Event.Added e) => { nextId++; items.Insert(0, e.Data); },
                        (Event.Updated e) =>
                        {
                            var i = items.FindIndex(item => item.Id == e.Data.Id);
                            if (i != -1)
                                items[i] = e.Data;
                        },
                        (Event.Deleted e) => items.RemoveAll(item => item.Id == e.Id),
                        (Event.Cleared e) => { nextId = e.NextId; items.Clear(); },
                        (Event.Compacted e) => { nextId = e.NextId; items = e.Items.ToList(); });
                return new State(nextId, items.ToArray());
            }

            /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
            public static bool IsOrigin(Event e) => e is Event.Cleared || e is Event.Compacted;

            /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
            public static Event Compact(State state) => new Event.Compacted { NextId = state.NextId, Items = state.Items };
        }

        /// Properties that can be edited on a Todo List item
        public class Props
        {
            public int Order { get; set; }
            public string Title { get; set; }
            public bool Completed { get; set; }
        }

        /// Defines the operations a caller can perform on a Todo List
        public abstract class Command
        : OneOfBase<Command.Add, Command.Update, Command.Delete, Command.Clear>
        {
            /// Create a single item
            public class Add : Command
            {
                public Props Props { get; set; }
            }

            /// Update a single item
            public class Update : Command
            {
                public int Id { get; set; }
                public Props Props { get; set; }
            }

            /// Delete a single item from the list
            public class Delete : Command
            {
                public int Id { get; set; }
            }

            /// Complete clear the todo list
            public class Clear : Command
            {
            }

            /// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream 
            public static IEnumerable<Event> Interpret(State s, Command x) =>
                x.Match(
                    (Add c) => new Event[] { Make<Event.Added>(s.NextId, c.Props) },
                    (Update c) =>
                    {
                        var proposed = new { c.Props.Order, c.Props.Title, c.Props.Completed };

                        bool IsEquivalent(Event.ItemData i) =>
                            i.Id == c.Id
                            && new { i.Order, i.Title, i.Completed } == proposed;

                        if (s.Items.Any(IsEquivalent))
                            return Enumerable.Empty<Event>();
                        return new[] { Make<Event.Updated>(c.Id, c.Props) };
                    },
                    (Delete c) => s.Items.Any(i => i.Id == c.Id) ? new Event[] { new Event.Deleted { Id = c.Id } } : new Event[0],
                    (Clear _) => s.Items.Any() ? new Event[] { new Event.Cleared { NextId = s.NextId } } : new Event[0]);

            static T Make<T>(int id, Props value) where T : Event.ItemEvent, new() =>
                new T { Data = { Id = id, Order = value.Order, Title = value.Title, Completed = value.Completed } };
        }

        /// Defines low level stream operations relevant to the Todo Stream in terms of Command and Events
        class Handler
        {
            readonly EquinoxStream<Event, State> _inner;

            public Handler(ILogger log, IStream<Event, State> stream) =>
                _inner = new EquinoxStream<Event, State>(State.Fold, log, stream);

            /// Execute `command`; does not emit the post state
            public Task<Unit> Execute(Command c) =>
                _inner.Execute(s => Command.Interpret(s, c));

            /// Handle `command`, return the items after the command's intent has been applied to the stream
            public Task<Event.ItemData[]> Decide(Command c) =>
                _inner.Decide(ctx =>
                {
                    ctx.Execute(s => Command.Interpret(s, c));
                    return ctx.State.Items;
                });

            /// Establish the present state of the Stream, project from that as specified by `projection`
            public Task<T> Query<T>(Func<State, T> projection) =>
                _inner.Query(projection);
        }

        /// A single Item in the Todo List
        public class View
        {
            public int Id { get; set; }
            public int Order { get; set; }
            public string Title { get; set; }
            public bool Completed { get; set; }
        }

        /// Defines operations that a Controller can perform on a Todo List
        public class Service
        {
            /// Maps a ClientId to Handler for the relevant stream
            readonly Func<ClientId, Handler> _stream;

            /// Maps a ClientId to the CatId that specifies the Stream in which the data for that client will be held
            static Target CategoryId(ClientId id) =>
                Target.NewAggregateId("Todos", id?.ToString() ?? "1");

            public Service(ILogger handlerLog, Func<Target, IStream<Event, State>> resolve) =>
                _stream = id => new Handler(handlerLog, resolve(CategoryId(id)));

            //
            // READ
            //

            /// List all open items
            public Task<IEnumerable<View>> List(ClientId clientId) =>
                _stream(clientId).Query(s => s.Items.Select(Render));

            /// Load details for a single specific item
            public Task<View> TryGet(ClientId clientId, int id) =>
                _stream(clientId).Query(s =>
                {
                    var i = s.Items.SingleOrDefault(x => x.Id == id);
                    return i == null ? null : Render(i);
                });

            //
            // WRITE
            //

            /// Execute the specified (blind write) command 
            public Task<Unit> Execute(ClientId clientId, Command command) =>
                _stream(clientId).Execute(command);

            //
            // WRITE-READ
            //

            /// Create a new ToDo List item; response contains the generated `id`
            public async Task<View> Create(ClientId clientId, Props template)
            {
                var state = await _stream(clientId).Decide(new Command.Add {Props = template});
                return Render(state.First());
            }

            /// Update the specified item as referenced by the `item.id`
            public async Task<View> Patch(ClientId clientId, int id, Props value)
            {
                var state = await _stream(clientId).Decide(new Command.Update {Id = id, Props = value});
                return Render(state.Single(x => x.Id == id));
            }

            static View Render(Event.ItemData i) =>
                new View {Id = i.Id, Order = i.Order, Title = i.Title, Completed = i.Completed};
        }
    }
}