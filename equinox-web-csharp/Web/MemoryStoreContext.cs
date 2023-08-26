using Equinox;
using Equinox.MemoryStore;
using Microsoft.FSharp.Core;
using System;
using System.Threading.Tasks;

namespace TodoBackendTemplate;

public class MemoryStoreContext : EquinoxContext
{
    readonly VolatileStore<ReadOnlyMemory<byte>> _store;

    public MemoryStoreContext(VolatileStore<ReadOnlyMemory<byte>> store) =>
        _store = store;

    public override Func<string, DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
        string name,
        Serilog.ILogger handlerLog,
        FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
        Func<TState,TEvent[], TState> fold,
        TState initial,
        Func<TEvent, bool> isOrigin = null,
        Func<TState, TEvent> toSnapshot = null)
    {
        var cat = new MemoryStoreCategory<TEvent, TState, ReadOnlyMemory<byte>, Unit>(_store, name, codec, fold, initial);
        return cat.Resolve(handlerLog);
    }

    internal override Task Connect() => Task.CompletedTask;
}