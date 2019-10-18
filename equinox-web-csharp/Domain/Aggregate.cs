using Equinox;
using Equinox.Core;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using OneOf;
using Serilog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public static class Aggregate
    {
        /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
        public abstract class Event : OneOfBase<Event.Happened, Event.Compacted>
        {
            public class Happened : Event
            {
            }

            public class Compacted : Event
            {
                public new bool Happened { get; set; }
            }

            static readonly JsonNetUtf8Codec Codec = new JsonNetUtf8Codec(new JsonSerializerSettings());
            
            public static Event TryDecode(string et, byte[] json)
            {
                switch (et)
                {
                    case nameof(Happened): return Codec.Decode<Happened>(json);
                    case nameof(Compacted): return Codec.Decode<Compacted>(json);
                    default: return null;
                }
            }

            public static Tuple<string, byte[]> Encode(Event e) => Tuple.Create(e.GetType().Name, Codec.Encode(e));
        }
        public class State
        {
            public bool Happened { get; set; }

            internal State(bool happened) { Happened = happened; }

            static void Evolve(State s, Event x) =>
                x.Match(
                    (Event.Happened _) => s.Happened = true,
                    (Event.Compacted e) => s.Happened = e.Happened);

            public static State Fold(State origin, IEnumerable<Event> xs)
            {
                // NB Fold must not mutate the origin
                var s = new State(origin.Happened);
                foreach (var x in xs)
                    Evolve(s, x);
                return s;
            }

            public static bool IsOrigin(Event e) => e is Event.Compacted;
            
            public static Event Compact(State s) => new Event.Compacted {Happened = s.Happened};
        }

        /// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream 
        public abstract class Command : OneOfBase<Command.MakeItSo>
        {
            public class MakeItSo : Command
            {
            }

            public static Event[] Interpret(State s, Command x) =>
                x.Match((MakeItSo _) => 
                    s.Happened ? new Event[0] : new Event [] { new Event.Happened()});
        }

        class Handler
        {
            readonly EquinoxStream<Event, State> _inner;

            public Handler(ILogger log, IStream<Event, State> stream) =>
                _inner = new EquinoxStream<Event, State>(State.Fold, log, stream);

            /// Execute `command`, syncing any events decided upon
            public Task<Unit> Execute(Command c) =>
                _inner.Execute(s => Command.Interpret(s, c));

            /// Establish the present state of the Stream, project from that as specified by `projection`
            public Task<T> Query<T>(Func<State, T> projection) =>
                _inner.Query(projection);
        }

        public class View
        {
            public bool Sorted { get; set; }
        }

        public class Service
        {
            /// Maps a ClientId to Handler for the relevant stream
            readonly Func<string, Handler> _stream;

            static Target AggregateId(string id) => Target.NewAggregateId("Aggregate", id);

            public Service(ILogger handlerLog, Func<Target, IStream<Event, State>> resolve) =>
                _stream = id => new Handler(handlerLog, resolve(AggregateId(id)));

            /// Execute the specified command 
            public Task<Unit> Execute(string id, Command command) =>
                _stream(id).Execute(command);

            /// Read the present state
            // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
            public Task<View> Read(string id) => _stream(id).Query(Render);

            static View Render(State s) => new View() {Sorted = s.Happened};
        }
    }
}