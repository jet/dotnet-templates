using Equinox;
using Equinox.Store;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public class Todo
    {
        public interface IEvent
        {
        }

        /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
        public static class Events
        {
            /// Information we retain per Todo List entry
            public abstract class ItemData
            {
                public int Id { get; set; }
                public int Order { get; set; }
                public string Title { get; set; }
                public bool Completed { get; set; }
            }

            public class Added : ItemData, IEvent
            {
            }

            public class Updated : ItemData, IEvent
            {
            }

            public class Deleted : IEvent
            {
                public int Id { get; set; }
            }

            public class Cleared : IEvent
            {
                public int NextId { get; set; }
            }

            public class Compacted : IEvent
            {
                public int NextId { get; set; }
                public ItemData[] Items { get; set; }
            }

            private static readonly JsonNetUtf8Codec Codec = new JsonNetUtf8Codec(new JsonSerializerSettings());

            public static IEvent TryDecode(string et, byte[] json)
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

            public static Tuple<string, byte[]> Encode(IEvent x)
            {
                switch (x)
                {
                    case Added e: return Tuple.Create(nameof(Added), Codec.Encode(e));
                    case Updated e: return Tuple.Create(nameof(Updated), Codec.Encode(e));
                    case Deleted e: return Tuple.Create(nameof(Deleted), Codec.Encode(e));
                    case Cleared e: return Tuple.Create(nameof(Cleared), Codec.Encode(e));
                    case Compacted e: return Tuple.Create(nameof(Compacted), Codec.Encode(e));
                    default: return null;
                }
            }
        }

        /// Present state of the Todo List as inferred from the Events we've seen to date
        // NB the value of the state is only ever manipulated in a cloned copy within Fold()
        // This is critical for caching and/or concurrent transactions to work correctly
        // In the F# impl, this is achieved by virtue of the fact that records and [F#] lists represent
        // persistent data structures https://en.wikipedia.org/wiki/Persistent_data_structure
        public class State
        {
            public int NextId { get; }
            public Events.ItemData[] Items { get; }

            public State(int nextId, Events.ItemData[] items)
            {
                NextId = nextId;
                Items = items;
            }
        }

        public static class Folds
        {
            public static State Initial = new State(0, new Events.ItemData[0]);

            /// Folds a set of events from the store into a given `state`
            public static State Fold(State origin, IEnumerable<IEvent> xs)
            {
                var nextId = origin.NextId;
                var items = origin.Items.ToList();
                foreach (var x in xs)
                    switch (x)
                    {
                        case Events.Added e:
                            nextId++;
                            items.Insert(0, e);
                            break;
                        case Events.Updated e:
                            var i = items.FindIndex(item => item.Id == e.Id);
                            if (i != -1)
                                items[i] = e;
                            break;
                        case Events.Deleted e:
                            items.RemoveAll(item => item.Id == e.Id);
                            break;
                        case Events.Cleared e:
                            nextId = e.NextId;
                            items.Clear();
                            break;
                        case Events.Compacted e:
                            nextId = e.NextId;
                            items = e.Items.ToList();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(x), x, "invalid");
                    }
                return new State(nextId, items.ToArray());
            }
            
            /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
            public static bool IsOrigin(IEvent e) => e is Events.Cleared || e is Events.Compacted;
            
            /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
            public static IEvent Compact(State state) => new Events.Compacted { NextId = state.NextId, Items = state.Items };
        }

        /// Properties that can be edited on a Todo List item
        public class Props
        {
            public int Order { get; set; }
            public string Title { get; set; }
            public bool Completed { get; set; }
        }

        /// Defines the operations a caller can perform on a Todo List
        public interface ICommand
        {
        }

        /// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream 
        public static class Commands
        {
            /// Create a single item
            public class Add : ICommand
            {
                public Props Props { get; set; }
            }

            /// Update a single item
            public class Update : ICommand
            {
                public int Id { get; set; }
                public Props Props { get; set; }
            }

            /// Delete a single item from the list
            public class Delete : ICommand
            {
                public int Id { get; set; }
            }

            /// Complete clear the todo list
            public class Clear : ICommand
            {
            }

            public static IEnumerable<IEvent> Interpret(State s, ICommand x)
            {
                switch (x)
                {
                    case Add c:
                        yield return Make<Events.Added>(s.NextId, c.Props);
                        break;
                    case Update c:
                        var proposed = Tuple.Create(c.Props.Order, c.Props.Title, c.Props.Completed);

                        bool IsEquivalent(Events.ItemData i) =>
                            i.Id == c.Id
                            && Tuple.Create(i.Order, i.Title, i.Completed).Equals(proposed);

                        if (!s.Items.Any(IsEquivalent))
                            yield return Make<Events.Updated>(c.Id, c.Props);
                        break;
                    case Delete c:
                        if (s.Items.Any(i => i.Id == c.Id))
                            yield return new Events.Deleted {Id = c.Id};
                        break;
                    case Clear c:
                        if (s.Items.Any()) yield return new Events.Cleared {NextId = s.NextId};
                        break;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(x), x, "invalid");
                }

                T Make<T>(int id, Props value) where T : Events.ItemData, IEvent, new() =>
                    new T() {Id = id, Order = value.Order, Title = value.Title, Completed = value.Completed};
            }
        }

        /// Defines low level stream operations relevant to the Todo Stream in terms of Command and Events
        private class Handler
        {
            readonly EquinoxHandler<IEvent, State> _inner;

            public Handler(ILogger log, IStream<IEvent, State> stream)
            {
                _inner = new EquinoxHandler<IEvent, State>(Folds.Fold, log, stream);
            }

            /// Execute `command`; does not emit the post state
            public Task<Unit> Execute(ICommand c) =>
                _inner.Execute(ctx =>
                    ctx.Execute(s => Commands.Interpret(s, c)));

            /// Handle `command`, return the items after the command's intent has been applied to the stream
            public Task<Events.ItemData[]> Decide(ICommand c) =>
                _inner.Decide(ctx =>
                {
                    ctx.Execute(s => Commands.Interpret(s, c));
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

            public Service(ILogger handlerLog, Func<Target, IStream<IEvent, State>> resolve) =>
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
            public Task<Unit> Execute(ClientId clientId, ICommand command) =>
                _stream(clientId).Execute(command);

            //
            // WRITE-READ
            //

            /// Create a new ToDo List item; response contains the generated `id`
            public async Task<View> Create(ClientId clientId, Props template)
            {
                var state = await _stream(clientId).Decide(new Commands.Add {Props = template});
                return Render(state.First());
            }

            /// Update the specified item as referenced by the `item.id`
            public async Task<View> Patch(ClientId clientId, int id, Props value)
            {
                var state = await _stream(clientId).Decide(new Commands.Update {Id = id, Props = value});
                return Render(state.Single(x => x.Id == id));
            }

            /// Maps a ClientId to the CatId that specifies the Stream in which the data for that client will be held
            static Target CategoryId(ClientId id) =>
                Target.NewCatId("Todos", id?.ToString() ?? "1");

            static View Render(Events.ItemData i) =>
                new View {Id = i.Id, Order = i.Order, Title = i.Title, Completed = i.Completed};
        }
    }
}