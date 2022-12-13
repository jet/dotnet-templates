using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public static class Aggregate
    {
        /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
        public abstract class Event
        {
            public class Happened : Event
            {
            }

            public class Snapshotted : Event
            {
                public bool HasHappened { get; set; }
            }

            static readonly SystemTextJsonUtf8Codec Codec = new(new());
            
            public static FSharpValueOption<Event> TryDecode(string et, ReadOnlyMemory<byte> json) =>
                et switch
                {
                    nameof(Happened) => Codec.Decode<Happened>(json),
                    nameof(Snapshotted) => Codec.Decode<Snapshotted>(json),
                    _ => FSharpValueOption<Event>.None
                };

            public static (string, ReadOnlyMemory<byte>) Encode(Event e) => (e.GetType().Name, Codec.Encode(e));
            public const string Category = "Aggregate"; 
            public static string StreamId(ClientId id) => id.ToString();
        }
        public class State
        {
            public bool Happened { get; set; }

            State(bool happened) { Happened = happened; }

            public static readonly State Initial = new (false);

            static void Evolve(State s, Event x) =>
                s.Happened = x switch
                {
                    Event.Happened => true,
                    Event.Snapshotted e => e.HasHappened,
                    _ => throw new ArgumentOutOfRangeException(nameof(x), x, "invalid")
                };

            public static State Fold(State origin, IEnumerable<Event> xs)
            {
                // NB Fold must not mutate the origin
                var s = new State(origin.Happened);
                foreach (var x in xs)
                    Evolve(s, x);
                return s;
            }

            public static bool IsOrigin(Event e) => e is Event.Snapshotted;
            
            public static Event Snapshot(State s) => new Event.Snapshotted {HasHappened = s.Happened};
        }

        /// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream 
        public abstract class Command
        {
            public class MakeItSo : Command
            {
            }

            public IEnumerable<Event> Interpret(State s)
            {
                switch (this)
                {
                    case MakeItSo:
                        if (!s.Happened) yield return new Event.Happened();
                        break;
                    default: throw new ArgumentOutOfRangeException(nameof(Command), this, "invalid");
                }
            }
        }

        public record View(bool Sorted);

        public class Service
        {
            /// Maps a ClientId to Handler for the relevant stream
            readonly Func<ClientId, Equinox.DeciderCore<Event, State>> _resolve;

            public Service(Func<ClientId, Equinox.DeciderCore<Event, State>> resolve) =>
                _resolve = resolve; 

            /// Execute the specified command 
            public Task<Unit> Execute(ClientId id, Command command) =>
                _resolve(id).Transact(command.Interpret);

            /// Read the present state
            // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
            public Task<View> Read(ClientId id) =>
                _resolve(id).Query(Render);

            static View Render(State s) =>
                new (Sorted: s.Happened);
        }
    }
}
