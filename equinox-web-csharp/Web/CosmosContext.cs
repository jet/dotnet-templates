using Equinox;
using Equinox.Cosmos;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using Serilog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public class CosmosConfig
    {
        public CosmosConfig(ConnectionMode mode, string connectionStringWithUriAndKey, string database,
            string container, int cacheMb)
        {
            Mode = mode;
            ConnectionStringWithUriAndKey = connectionStringWithUriAndKey;
            Database = database;
            Container = container;
            CacheMb = cacheMb;
        }

        public ConnectionMode Mode { get; }
        public string ConnectionStringWithUriAndKey { get; }
        public string Database { get; }
        public string Container { get; }
        public int CacheMb { get; }
    }

    public class CosmosContext : EquinoxContext
    {
        readonly Cache _cache;

        Context _store;
        readonly Func<Task> _connect;

        public CosmosContext(CosmosConfig config)
        {
            _cache = new Cache("Cosmos", config.CacheMb);
            var retriesOn429Throttling = 1; // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            var timeout = TimeSpan.FromSeconds(5); // Timeout applied per request to CosmosDb, including retry attempts
            var discovery = Discovery.FromConnectionString(config.ConnectionStringWithUriAndKey);
            _connect = async () =>
            {
                var gateway = await Connect("App", config.Mode, discovery, timeout, retriesOn429Throttling, timeout);
                var containers = new Containers(config.Database, config.Container);

                _store = new Context(gateway, containers);
            };
        }

        internal override async Task Connect() => await _connect();

        static async Task<Gateway> Connect(string appName, ConnectionMode mode, Discovery discovery, TimeSpan operationTimeout,
            int maxRetryForThrottling, TimeSpan maxRetryWait)
        {
            var log = Log.ForContext<CosmosContext>();
            var c = new Connector(operationTimeout, maxRetryForThrottling, maxRetryWait, log, mode: mode);
            var conn = await FSharpAsync.StartAsTask(c.Connect(appName, discovery), null, null);
            return new Gateway(conn, new BatchingPolicy(defaultMaxItems: 500));
        }

        public override Func<string, Equinox.Core.IStream<TEvent, TState>> Resolve<TEvent, TState>(
            FsCodec.IEventCodec<TEvent, byte[], object> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null)
        {
            var accessStrategy =
                isOrigin == null && compact == null
                    ? null
                    : AccessStrategy<TEvent, TState>.NewSnapshot(FuncConvert.FromFunc(isOrigin), FuncConvert.FromFunc(compact));

            var cacheStrategy = _cache == null
                ? null
                : CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
            var resolver = new Resolver<TEvent, TState, object>(_store, codec, FuncConvert.FromFunc(fold), initial, cacheStrategy, accessStrategy);
            return t => resolver.Resolve(t);
        }
    }
}