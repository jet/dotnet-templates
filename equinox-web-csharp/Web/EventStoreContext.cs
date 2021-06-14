using Equinox;
using Equinox.EventStore;
using Equinox.Core;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
     public class EventStoreConfig
    {
        public EventStoreConfig(string host, string username, string password, int cacheMb)
        {
            Host = host;
            Username = username;
            Password = password;
            CacheMb = cacheMb;
        }

        public string Host { get; }
        public string Username { get; }
        public string Password { get; }
        public int CacheMb { get; }
    }

    public class EventStoreContext : EquinoxContext
    {
        readonly Cache _cache;

        Equinox.EventStore.EventStoreContext _connection;
        readonly Func<Task> _connect;

        public EventStoreContext(EventStoreConfig config)
        {
            _cache = new Cache("Es", config.CacheMb);
            _connect = async () => _connection = await Connect(config);
        }

        internal override async Task Connect() => await _connect();

        static async Task<Equinox.EventStore.EventStoreContext> Connect(EventStoreConfig config)
        {
            var log = Logger.NewSerilogNormal(Serilog.Log.ForContext<EventStoreContext>());
            var c = new Connector(config.Username, config.Password, reqTimeout: TimeSpan.FromSeconds(5), reqRetries: 1);

            var conn = await FSharpAsync.StartAsTask(
                c.Establish("Twin", Discovery.NewGossipDns(config.Host), ConnectionStrategy.ClusterTwinPreferSlaveReads),
                null, null);
            return new Equinox.EventStore.EventStoreContext(conn, new BatchingPolicy(maxBatchSize: 500));
        }

        public override Func<string, IStream<TEvent, TState>> Resolve<TEvent, TState>(
            FsCodec.IEventCodec<TEvent, byte[], object> codec,
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
            var cat = new EventStoreCategory<TEvent, TState, object>(_connection, codec, FuncConvert.FromFunc(fold),
                initial, cacheStrategy, accessStrategy);
            return t => cat.Resolve(t);
        }
    }
}