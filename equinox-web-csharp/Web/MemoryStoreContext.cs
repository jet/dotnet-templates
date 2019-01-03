using System;
using System.Collections.Generic;
using Equinox.MemoryStore;
using Equinox.UnionCodec;
using Microsoft.FSharp.Core;

namespace TodoBackendTemplate
{
    public class MemoryStoreContext : EquinoxContext
    {
        readonly VolatileStore _store;

        public MemoryStoreContext(VolatileStore store)
        {
            _store = store;
        }

        public override Equinox.Store.IStream<TEvent, TState> Resolve<TEvent, TState>(
            IUnionEncoder<TEvent, byte[]> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Equinox.Target target,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null)
        {
            var resolver = new MemResolver<TEvent, TState>(_store, FuncConvert.FromFunc(fold), initial);
            return resolver.Resolve.Invoke(target);
        }

        internal override void Connect()
        {
        }
    }
}