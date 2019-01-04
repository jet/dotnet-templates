using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Equinox;
using Equinox.Store;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using Serilog;

namespace TodoBackendTemplate
{
    public class Aggregate
    {
        public interface IEvent
        {
        }

        /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
        public static class Events
        {
            public class Happened : IEvent
            {
            }

            public class Compacted : IEvent
            {
                public bool Happened { get; set; }
            }

            static JsonNetUtf8Codec _codec = new JsonNetUtf8Codec(new JsonSerializerSettings());
            
            public static IEvent TryDecode(string et, byte[] json)
            {
                switch (et)
                {
                    case "Happened": return _codec.Decode<Happened>(json);
                    case "Compacted": return _codec.Decode<Compacted>(json);
                    default: return null;
                }
            }
            
            public static Tuple<string,byte[]> Encode(IEvent x)
            {
                switch (x)
                {
                    case Happened e: return Tuple.Create("Happened", _codec.Encode(e));
                    case Compacted e: return Tuple.Create("Compacted", _codec.Encode(e));
                    default: return null;
                }
            }
        }

        public class State
        {
            public bool Happened { get; set; }
        }

        public static class Folds
        {
            public static State Initial = new State {Happened = false};

            private static void Evolve(State s, IEvent x)
            {
                switch (x)
                {
                    case Events.Happened e:
                        s.Happened = true;
                        break;
                    case Events.Compacted e:
                        s.Happened = e.Happened;
                        break;
                    default: throw new ArgumentOutOfRangeException(nameof(x), x, "invalid");
                }
            }

            public static State Fold(State origin, IEnumerable<IEvent> xs)
            {
                var s = new State {Happened = origin.Happened};
                foreach (var x in xs) Evolve(s, x);
                return s;
            }

            public static bool IsOrigin(IEvent e) => e is Events.Compacted;

            public static IEvent Compact(State s) => new Events.Compacted {Happened = s.Happened};
        }

        interface ICommand
        {
        }

        static class Commands
        {
            class MakeItSo : ICommand
            {
            }

            public static IEnumerable<IEvent> Interpret(State s, ICommand x)
            {
                switch (x)
                {
                    case MakeItSo c:
                        if (!s.Happened) yield return new Events.Happened();
                        break;
                    default: throw new ArgumentOutOfRangeException(nameof(x), x, "invalid");
                }
            }
        }


        class Handler
        {
            readonly EquinoxHandler<IEvent, State> _inner;

            public Handler(ILogger log, IStream<IEvent, State> stream)
            {
                _inner = new EquinoxHandler<IEvent, State>(Folds.Fold, log, stream);
            }

            /// Execute `command`, syncing any events decided upon
            public Task<Unit> Execute(ICommand c) =>
                _inner.Decide(ctx =>
                    ctx.Execute(s => Commands.Interpret(s, c)));

            /// Establish the present state of the Stream, project from that as specified by `projection`
            public Task<T> Query<T>(Func<State, T> projection) =>
                _inner.Query(projection);
        }

        class View
        {
            public bool Sorted { get; private set; }
        }

        public class Service
        {
            /// Maps a ClientId to Handler for the relevant stream
            readonly Func<string, Handler> _stream;

            public Service(ILogger handlerLog, Func<Target, IStream<IEvent, State>> resolve) =>
                _stream = id => new Handler(handlerLog, resolve(CategoryId(id)));

            static Target CategoryId(string id) => Target.NewCatId("Aggregate", id);

            static View Render(State s) => new View() {Sorted = s.Happened};

            /// Read the present state
            // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
            public Task<View> Read(string id) => _stream(id).Query(Render);

            /// Execute the specified command 
            public Task<Unit> Execute(string id, ICommand command) =>
                _stream(id).Execute(command);
        }
    }
}