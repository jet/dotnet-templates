using Equinox;
using Equinox.EventStoreDb;
using Microsoft.FSharp.Core;

namespace TodoBackendTemplate;

public record EventStoreConfig(string ConnectionString, int CacheMb);

public class EventStoreContext(EventStoreConfig config) : EquinoxContext
{
    readonly Cache _cache = new("Es", config.CacheMb);
    Equinox.EventStoreDb.EventStoreContext _connection = null!;

    internal override async Task Connect() =>
        _connection = await ConnectStore(config);

    static Task<Equinox.EventStoreDb.EventStoreContext> ConnectStore(EventStoreConfig config)
    {
        var c = new EventStoreConnector(reqTimeout: TimeSpan.FromSeconds(5));
        var conn = c.Establish("Twin", Discovery.NewConnectionString(config.ConnectionString), ConnectionStrategy.ClusterTwinPreferSlaveReads);
        return Task.FromResult(new Equinox.EventStoreDb.EventStoreContext(conn));
    }

    public override Func<string, DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
        string name,
        Serilog.ILogger handlerLog,
        FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
        Func<TState, TEvent[], TState> fold,
        TState initial,
        Func<TEvent, bool>? isOrigin = null,
        Func<TState, TEvent>? toSnapshot = null)
    {
        var accessStrategy =
            isOrigin == null && toSnapshot == null
                ? null
                : AccessStrategy<TEvent, TState>.NewRollingSnapshots(FuncConvert.FromFunc(isOrigin!), FuncConvert.FromFunc(toSnapshot!));
        var cacheStrategy = CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
        var cat = new EventStoreCategory<TEvent, TState, Unit>(_connection, name, codec, fold, initial, accessStrategy, cacheStrategy);
        return cat.Resolve(handlerLog);
    }
}