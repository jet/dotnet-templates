using Microsoft.FSharp.Core;

namespace TodoBackendTemplate;

public static class Aggregate
{
    public interface IEvent { }
    /// NB - these types and names reflect the actual storage formats and hence need to be versioned with care
    public static class Event
    {
        public record Happened : IEvent { }
        public record Snapshotted(bool HasHappened) : IEvent;

        static readonly SystemTextJsonUtf8Codec Codec = new(new System.Text.Json.JsonSerializerOptions());

        public static IEvent? TryDecode(string et, ReadOnlyMemory<byte> json) =>
            et switch
            {
                nameof(Happened) => Codec.Decode<Happened>(json),
                nameof(Snapshotted) => Codec.Decode<Snapshotted>(json),
                _ => null
            };

        public static (string, ReadOnlyMemory<byte>) Encode(IEvent e) => (e.GetType().Name, Codec.Encode(e));
        public const string Category = "Aggregate";
        public static string StreamId(ClientId id) => id.ToString();
    }

    public class State
    {
        public bool Happened { get; set; }

        State(bool happened) { Happened = happened; }

        public static readonly State Initial = new(false);

        static void Evolve(State s, IEvent x) =>
            s.Happened = x switch
            {
                Event.Happened => true,
                Event.Snapshotted e => e.HasHappened,
                _ => throw new ArgumentOutOfRangeException(nameof(x), x, "invalid")
            };

        public static State Fold(State origin, IEnumerable<IEvent> xs)
        {
            // NB Fold must not mutate the origin
            var s = new State(origin.Happened);
            foreach (var x in xs)
                Evolve(s, x);
            return s;
        }

        public static bool IsOrigin(IEvent e) => e is Event.Snapshotted;

        public static IEvent Snapshot(State s) => new Event.Snapshotted(s.Happened);
    }

    /// Defines the decision process which maps from the intent of the `Command` to the `Event`s that represent that decision in the Stream
    public abstract class Command
    {
        public class MakeItSo : Command { }

        public IEvent[] Interpret(State s) => this switch
        {
            MakeItSo => s.Happened ? [] : [new Event.Happened()],
            _ => throw new ArgumentOutOfRangeException(nameof(Command), this, "invalid")
        };
    }

    public record View(bool Sorted);

    public class Service(Func<ClientId, Equinox.DeciderCore<IEvent, State>> resolve)
    {
        /// Execute the specified command
        public Task<Unit> Execute(ClientId id, Command command) =>
            resolve(id).Transact(command.Interpret);

        /// Read the present state
        // TOCONSIDER: you should probably be separating this out per CQRS and reading from a denormalized/cached set of projections
        public Task<View> Read(ClientId id) =>
            resolve(id).Query(Render);

        static View Render(State s) => new(Sorted: s.Happened);
    }
}