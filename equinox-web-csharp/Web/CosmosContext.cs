using Equinox;
using Equinox.Cosmos;
using Equinox.UnionCodec;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using Serilog;
using System;
using System.Collections.Generic;

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
        readonly Lazy<EqxStore> _store;
        readonly Caching.Cache _cache;

        public CosmosContext(CosmosConfig config)
        {
            _cache = new Caching.Cache("Cosmos", config.CacheMb);
            var retriesOn429Throttling = 1; // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            var timeout = TimeSpan.FromSeconds(5); // Timeout applied per request to CosmosDb, including retry attempts
            var discovery = Discovery.FromConnectionString(config.ConnectionStringWithUriAndKey);
           _store = new Lazy<EqxStore>(() =>
           {
               var gateway = Connect("App", config.Mode, discovery, timeout, retriesOn429Throttling,
                   (int) timeout.TotalSeconds);
               var collectionMapping = new EqxCollections(config.Database, config.Collection);

               return new EqxStore(gateway, collectionMapping);
           });
    }

        private static EqxGateway Connect(string appName, ConnectionMode mode, Discovery discovery, TimeSpan operationTimeout,
            int maxRetryForThrottling, int maxRetryWaitSeconds)
        {
            var log = Log.ForContext<CosmosContext>();
            var c = new EqxConnector(operationTimeout, maxRetryForThrottling, maxRetryWaitSeconds, log, mode: mode);
            var conn = FSharpAsync.RunSynchronously(c.Connect(appName, discovery), null, null);
            return new EqxGateway(conn, new EqxBatchingPolicy(defaultMaxItems: 500));
        }

        internal override void Connect()
        {
            var _ = _store.Value;
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
            var resolver = new EqxResolver<TEvent, TState>(_store.Value, codec, FuncConvert.FromFunc(fold), initial, accessStrategy, cacheStrategy);
            return t => resolver.Resolve.Invoke(t);
        }
    }
}