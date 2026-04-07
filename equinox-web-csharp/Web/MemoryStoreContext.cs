using Equinox;
using Equinox.MemoryStore;
using Microsoft.FSharp.Core;

namespace TodoBackendTemplate;

public class MemoryStoreContext(VolatileStore<ReadOnlyMemory<byte>> store) : EquinoxContext
{
    public override Func<string, DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
        string name,
        Serilog.ILogger handlerLog,
        FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
        Func<TState, TEvent[], TState> fold,
        TState initial,
        Func<TEvent, bool>? isOrigin = null,
        Func<TState, TEvent>? toSnapshot = null)
    {
        var cat = new MemoryStoreCategory<TEvent, TState, ReadOnlyMemory<byte>, Unit>(store, name, codec, fold, initial);
        return cat.Resolve(handlerLog);
    }

    internal override Task Connect() => Task.CompletedTask;
}