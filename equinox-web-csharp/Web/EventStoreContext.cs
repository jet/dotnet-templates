using Equinox;
using Equinox.EventStore;
using Equinox.Store;
using Equinox.UnionCodec;
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
        readonly Caching.Cache _cache;

        GesGateway _gateway;
        readonly Func<Task> _connect;

        public EventStoreContext(EventStoreConfig config)
        {
            _cache = new Caching.Cache("Es", config.CacheMb);
            _connect = async () => _gateway = await Connect(config);
        }

        internal override async Task Connect() => await _connect();

        static async Task<GesGateway> Connect(EventStoreConfig config)
        {
            var log = Logger.NewSerilogNormal(Serilog.Log.ForContext<EventStoreContext>());
            var c = new GesConnector(config.Username, config.Password, reqTimeout: TimeSpan.FromSeconds(5), reqRetries: 1);

            var conn = await FSharpAsync.StartAsTask(
                c.Establish("Twin", Discovery.NewGossipDns(config.Host), ConnectionStrategy.ClusterTwinPreferSlaveReads),
                null, null);
            return new GesGateway(conn, new GesBatchingPolicy(maxBatchSize: 500));
        }

        public override Func<Target,IStream<TEvent, TState>> Resolve<TEvent, TState>(
            IUnionEncoder<TEvent, byte[]> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null)
        {
            var accessStrategy =
                isOrigin == null && compact == null
                    ? null
                    : AccessStrategy<TEvent, TState>.NewRollingSnapshots(FuncConvert.FromFunc(isOrigin), FuncConvert.FromFunc(compact));
            var cacheStrategy = _cache == null
                ? null
                : CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
            var resolver = new GesResolver<TEvent, TState>(_gateway, codec, FuncConvert.FromFunc(fold),
                initial, accessStrategy, cacheStrategy);
            return t => resolver.Resolve.Invoke(t);
        }
    }
}