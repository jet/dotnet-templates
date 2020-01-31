using Equinox;
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
        readonly VolatileStore<object> _store;

        public MemoryStoreContext(VolatileStore<object> store) =>
            _store = store;

        public override Func<string, IStream<TEvent, TState>> Resolve<TEvent, TState>(
            FsCodec.IEventCodec<TEvent, byte[], object> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null)
        {
            var resolver = new Resolver<TEvent, TState, object, object>(_store, FsCodec.Box.Codec.Create<TEvent>(), FuncConvert.FromFunc(fold), initial);
            return target => resolver.Resolve(target);
        }

        internal override Task Connect() => Task.CompletedTask;
    }
}