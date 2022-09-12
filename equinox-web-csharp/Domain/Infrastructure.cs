using Equinox;
using Equinox.Core;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public class Accumulator<TEvent,TState>
    {
        readonly Func<TState, IEnumerable<TEvent>, TState> _fold;
        readonly TState _state;
        public List<TEvent> Accumulated { get; } = new List<TEvent>();

        public Accumulator(Func<TState,IEnumerable<TEvent>,TState> fold, TState state)
        {
            _fold = fold;
            _state = state;
        }

        public TState State => _fold(_state,Accumulated);

        public void Execute(Func<TState, IEnumerable<TEvent>> f) => Accumulated.AddRange(f(State));
    }

    public class EquinoxStream<TEvent, TState> : DeciderCore<TEvent, TState>
    {
        private readonly Func<TState, IEnumerable<TEvent>, TState> _fold;

        public EquinoxStream(
                Func<TState, IEnumerable<TEvent>, TState> fold,
                IStream<TEvent, TState> stream)
            : base(stream)
        {
            _fold = fold;
        }

        /// Run the decision method, letting it decide whether or not the Command's intent should manifest as Events
        public async Task<Unit> Execute(Func<TState, IEnumerable<TEvent>> interpret)
        {
            return await Transact(interpret);
        }

        /// Execute a command, as Decide(Action) does, but also yield an outcome from the decision
        public async Task<T> Decide<T>(Func<Accumulator<TEvent, TState>, T> decide)
        {
            (T, IEnumerable<TEvent>) decideWrapped(TState state)
            {
                var a = new Accumulator<TEvent, TState>(_fold, state);
                var r = decide(a);
                return (r, a.Accumulated);
            }

            return await Transact(decide: decideWrapped);
        }

        // Project from the synchronized state, without the possibility of adding events that Decide(Func) admits
        public async Task<T> Query<T>(Func<TState, T> project) =>
            await base.Query(project);
    }

    /// System.Text.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
    public class SystemTextJsonUtf8Codec
    {
        private readonly TypeShape.UnionContract.IEncoder<ReadOnlyMemory<byte>> _codec; 

        public SystemTextJsonUtf8Codec(System.Text.Json.JsonSerializerOptions options) =>
            _codec = new FsCodec.SystemTextJson.Core.ReadOnlyMemoryEncoder(options);

        public ReadOnlyMemory<byte> Encode<T>(T value) where T : class => _codec.Encode(value);

        public T Decode<T>(ReadOnlyMemory<byte> json) where T : class => _codec.Decode<T>(json);
    }
}
