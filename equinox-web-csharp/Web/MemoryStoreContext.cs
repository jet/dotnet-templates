using Equinox.MemoryStore;
using Equinox.Core;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public class MemoryStoreContext : EquinoxContext
    {
        readonly VolatileStore<byte[]> _store;

        public MemoryStoreContext(VolatileStore<byte[]> store) =>
            _store = store;

        public override Func<string, IStream<TEvent, TState>> Resolve<TEvent, TState>(
            FsCodec.IEventCodec<TEvent, byte[], object> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> toSnapshot = null)
        {
            var resolver = new Resolver<TEvent, TState, byte[], object>(_store, codec, FuncConvert.FromFunc(fold), initial);
            return target => resolver.Resolve(target);
        }

        internal override Task Connect() => Task.CompletedTask;
    }
}