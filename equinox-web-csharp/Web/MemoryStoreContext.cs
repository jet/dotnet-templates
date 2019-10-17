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
        readonly VolatileStore _store;

        public MemoryStoreContext(VolatileStore store) =>        
            _store = store;

        public override Func<Target,IStream<TEvent, TState>> Resolve<TEvent, TState>(
            FsCodec.IUnionEncoder<TEvent, byte[], object> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null)
        {
            var resolver = new Resolver<TEvent, TState, object>(_store, FuncConvert.FromFunc(fold), initial);
            return target => resolver.Resolve(target);
        }

        internal override Task Connect() => Task.CompletedTask;
    }
}