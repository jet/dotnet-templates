using Equinox;
using Equinox.MemoryStore;
using Equinox.Store;
using Equinox.UnionCodec;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;

namespace TodoBackendTemplate
{
    public class MemoryStoreContext : EquinoxContext
    {
        readonly VolatileStore _store;

        public MemoryStoreContext(VolatileStore store)
        {
            _store = store;
        }

        public override Func<Target,IStream<TEvent, TState>> Resolve<TEvent, TState>(
            IUnionEncoder<TEvent, byte[]> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null)
        {
            var resolver = new MemResolver<TEvent, TState>(_store, FuncConvert.FromFunc(fold), initial);
            return target => resolver.Resolve.Invoke(target);
        }

        internal override void Connect()
        {
        }
    }
}