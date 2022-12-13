using Equinox;
using Equinox.MemoryStore;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public class MemoryStoreContext : EquinoxContext
    {
        readonly VolatileStore<ReadOnlyMemory<byte>> _store;

        public MemoryStoreContext(VolatileStore<ReadOnlyMemory<byte>> store) =>
            _store = store;

        public override Func<(string, string), DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
            Serilog.ILogger handlerLog,
            FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> toSnapshot = null)
        {
            var cat = new MemoryStoreCategory<TEvent, TState, ReadOnlyMemory<byte>, Unit>(_store, codec, FuncConvert.FromFunc(fold), initial);
            return args => cat.Resolve(handlerLog).Invoke(args.Item1, args.Item2);
        }

        internal override Task Connect() => Task.CompletedTask;
    }
}
