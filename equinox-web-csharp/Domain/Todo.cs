using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public static class Todo
    {
        /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
        public abstract class Event
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
                public int Id { get; init; }
            }

            public class Cleared : Event
            {
                public int NextId { get; init; }
            }

            public class Snapshotted : Event
            {
                public int NextId { get; init; }
                public ItemData[] Items { get; init; }
            }

            static readonly SystemTextJsonUtf8Codec Codec =
                new (new JsonSerializerOptions());

            public static FSharpValueOption<Event> TryDecode(string et, ReadOnlyMemory<byte> json) =>
                et switch
                {
                    nameof(Added) => Codec.Decode<Added>(json),
                    nameof(Updated) => Codec.Decode<Updated>(json),
                    nameof(Deleted) => Codec.Decode<Deleted>(json),
                    nameof(Cleared) => Codec.Decode<Cleared>(json),
                    nameof(Snapshotted) => Codec.Decode<Snapshotted>(json),
                    _ => FSharpValueOption<Event>.None
                };

            public static (string, ReadOnlyMemory<byte>) Encode(Event e) =>
                (e.GetType().Name, Codec.Encode(e));

            /// Maps a ClientId to the Target that specifies the Stream in which the data for that client will be held
            public const string Category = "Todos"; 
            public static string StreamId(ClientId id) => id?.ToString() ?? "1";
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

            private State(int nextId, Event.ItemData[] items)
            {
                NextId = nextId;
                Items = items;
            }

            public static readonly State Initial = new (0, Array.Empty<Event.ItemData>());

            /// Folds a set of events from the store into a given `state`
            public static State Fold(State origin, IEnumerable<Event> xs)
            {
                var nextId = origin.NextId;
                var items = origin.Items.ToList();
                foreach (var x in xs)
                    switch (x)
                    {
                        case Event.Added e:
                            nextId++;
                            items.Insert(0, e.Data);
                            break;
                        case Event.Updated e:
                            var i = items.FindIndex(item => item.Id == e.Data.Id);
                            if (i != -1)
                                items[i] = e.Data;
                            break;
                        case Event.Deleted e:
                            items.RemoveAll(item => item.Id == e.Id);
                            break;
                        case Event.Cleared e:
                            nextId = e.NextId;
                            items.Clear();
                            break;
                        case Event.Snapshotted e:
                            nextId = e.NextId;
                            items = e.Items.ToList();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(Command), x, "invalid");
                    }
                return new State(nextId, items.ToArray());
            }
            
            /// Determines whether a given event represents a checkpoint that implies we don't need to see any preceding events
            public static bool IsOrigin(Event e) => e is Event.Cleared || e is Event.Snapshotted;
            
            /// Prepares an Event that encodes all relevant aspects of a State such that `evolve` can rehydrate a complete State from it
            public static Event Snapshot(State state) => new Event.Snapshotted { NextId = state.NextId, Items = state.Items };
        }

        /// Properties that can be edited on a Todo List item
        public class Props
        {
            public int Order { get; init; }
            public string Title { get; init; }
            public bool Completed { get; init; }
        }

        /// Defines the operations a caller can perform on a Todo List
        public abstract class Command
        {
            /// Create a single item
            public class Add : Command
            {
                public Props Props { get; init; }
            }

            /// Update a single item
            public class Update : Command
            {
                public int Id { get; init; }
                public Props Props { get; init; }
            }

            /// Delete a single item from the list
            public class Delete : Command
            {
                public int Id { get; init; }
            }

            /// Completely clear the list
            public class Clear : Command
            {
            }

            /// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream 
            public IEnumerable<Event> Interpret(State s)
            {
                switch (this)
                {
                    case Add c:
                        yield return Make<Event.Added>(s.NextId, c.Props);
                        break;
                    case Update c:
                        var proposed = new {c.Props.Order, c.Props.Title, c.Props.Completed};

                        bool IsEquivalent(Event.ItemData i) =>
                            i.Id == c.Id
                            && new {i.Order, i.Title, i.Completed} == proposed;

                        if (!s.Items.Any(IsEquivalent))
                            yield return Make<Event.Updated>(c.Id, c.Props);
                        break;
                    case Delete c:
                        if (s.Items.Any(i => i.Id == c.Id))
                            yield return new Event.Deleted {Id = c.Id};
                        break;
                    case Clear:
                        if (s.Items.Any())
                            yield return new Event.Cleared {NextId = s.NextId};
                        break;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(s), this, "invalid");
                }

                T Make<T>(int id, Props value) where T : Event.ItemEvent, new() =>
                    new T {Data = {Id = id, Order = value.Order, Title = value.Title, Completed = value.Completed}};
            }
        }

        /// A single Item in the Todo List
        public class View
        {
            public int Id { get; init; }
            public int Order { get; init; }
            public string Title { get; init; }
            public bool Completed { get; init; }
        }

        /// Defines operations that a Controller can perform on a Todo List
        public class Service
        {
            /// Maps a ClientId to Handler for the relevant stream
            readonly Func<ClientId, Equinox.DeciderCore<Event, State>> _resolve;

            public Service(Func<ClientId, Equinox.DeciderCore<Event, State>> resolve) =>
                _resolve = resolve;

            //
            // READ
            //

            /// List all open items
            public Task<IEnumerable<View>> List(ClientId clientId) =>
                _resolve(clientId).Query(s => s.Items.Select(Render));

            /// Load details for a single specific item
            public Task<View> TryGet(ClientId clientId, int id) =>
                _resolve(clientId).Query(s =>
                {
                    var i = s.Items.SingleOrDefault(x => x.Id == id);
                    return i == null ? null : Render(i);
                });

            //
            // WRITE
            //

            /// Execute the specified (blind write) command 
            public Task<Unit> Execute(ClientId clientId, Command command) =>
                _resolve(clientId).Transact(command.Interpret);

            //
            // WRITE-READ
            //

            /// Create a new ToDo List item; response contains the generated `id`
            public Task<View> Create(ClientId clientId, Props template) =>
                _resolve(clientId).Transact(
                    new Command.Add {Props = template}.Interpret, 
                    s => Render(s.Items.First()));

            /// Update the specified item as referenced by the `item.id`
            public Task<View> Patch(ClientId clientId, int id, Props value) =>
                _resolve(clientId).Transact(
                    new Command.Update {Id = id, Props = value}.Interpret,
                    s => Render(s.Items.Single(x => x.Id == id)));

                static View Render(Event.ItemData i) =>
                    new View {Id = i.Id, Order = i.Order, Title = i.Title, Completed = i.Completed};
        }
    }
}
