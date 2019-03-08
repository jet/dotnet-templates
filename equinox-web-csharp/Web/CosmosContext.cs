using Equinox;
using Equinox.Cosmos;
using Equinox.Codec.JsonNet;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using Serilog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Equinox.Codec;

namespace TodoBackendTemplate
{
    public class CosmosConfig
    {
        public CosmosConfig(ConnectionMode mode, string connectionStringWithUriAndKey, string database,
            string collection, int cacheMb)
        {
            Mode = mode;
            ConnectionStringWithUriAndKey = connectionStringWithUriAndKey;
            Database = database;
            Collection = collection;
            CacheMb = cacheMb;
        }

        public ConnectionMode Mode { get; }
        public string ConnectionStringWithUriAndKey { get; }
        public string Database { get; }
        public string Collection { get; }
        public int CacheMb { get; }
    }

    public class CosmosContext : EquinoxContext
    {
        readonly Caching.Cache _cache;

        CosmosStore _store;
        readonly Func<Task> _connect;

        public CosmosContext(CosmosConfig config)
        {
            _cache = new Caching.Cache("Cosmos", config.CacheMb);
            var retriesOn429Throttling = 1; // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            var timeout = TimeSpan.FromSeconds(5); // Timeout applied per request to CosmosDb, including retry attempts
            var discovery = Discovery.FromConnectionString(config.ConnectionStringWithUriAndKey);
            _connect = async () =>
            {
                var gateway = await Connect("App", config.Mode, discovery, timeout, retriesOn429Throttling,
                    (int)timeout.TotalSeconds);
                var collectionMapping = new CosmosCollections(config.Database, config.Collection);

                _store = new CosmosStore(gateway, collectionMapping);
            };
        }

        internal override async Task Connect() => await _connect();

        static async Task<CosmosGateway> Connect(string appName, ConnectionMode mode, Discovery discovery, TimeSpan operationTimeout,
            int maxRetryForThrottling, int maxRetryWaitSeconds)
        {
            var log = Log.ForContext<CosmosContext>();
            var c = new CosmosConnector(operationTimeout, maxRetryForThrottling, maxRetryWaitSeconds, log, mode: mode);
            var conn = await FSharpAsync.StartAsTask(c.Connect(appName, discovery), null, null);
            return new CosmosGateway(conn, new CosmosBatchingPolicy(defaultMaxItems: 500));
        }

        public override Func<Target,Equinox.Store.IStream<TEvent, TState>> Resolve<TEvent, TState>(
            IUnionEncoder<TEvent, byte[]> codec,
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
            var resolver = new CosmosResolver<TEvent, TState>(_store, codec, FuncConvert.FromFunc(fold), initial, cacheStrategy, accessStrategy);
            return t => resolver.Resolve.Invoke(t);
        }
    }
}