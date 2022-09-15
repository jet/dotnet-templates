using Equinox;
using Equinox.EventStoreDb;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public record EventStoreConfig(string ConnectionString, int CacheMb);

    public class EventStoreContext : EquinoxContext
    {
        readonly Cache _cache;

        Equinox.EventStoreDb.EventStoreContext _connection;
        readonly Func<Task> _connect;

        public EventStoreContext(EventStoreConfig config)
        {
            _cache = new Cache("Es", config.CacheMb);
            _connect = async () => _connection = await Connect(config);
        }

        internal override async Task Connect() => await _connect();

        static Task<Equinox.EventStoreDb.EventStoreContext> Connect(EventStoreConfig config)
        {
            var c = new EventStoreConnector(reqTimeout: TimeSpan.FromSeconds(5), reqRetries: 1);

            var conn = c.Establish("Twin", Discovery.NewConnectionString(config.ConnectionString), ConnectionStrategy.ClusterTwinPreferSlaveReads);
            return Task.FromResult(new Equinox.EventStoreDb.EventStoreContext(conn));
        }

        public override Func<(string, string), DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
            Serilog.ILogger handlerLog,
            FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> toSnapshot = null)
        {
            var accessStrategy =
                isOrigin == null && toSnapshot == null
                    ? null
                    : AccessStrategy<TEvent, TState>.NewRollingSnapshots(FuncConvert.FromFunc(isOrigin), FuncConvert.FromFunc(toSnapshot));
            var cacheStrategy = _cache == null
                ? null
                : CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
            var cat = new EventStoreCategory<TEvent, TState, Unit>(_connection, codec, FuncConvert.FromFunc(fold),
                initial, cacheStrategy, accessStrategy);
            return cat.Resolve(log: handlerLog);
        }
    }
}
