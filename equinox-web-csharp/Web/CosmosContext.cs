using Equinox;
using Equinox.CosmosStore;
using FsCodec.SystemTextJson.Interop;
using Microsoft.Azure.Cosmos;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public record CosmosConfig(ConnectionMode Mode, string ConnectionStringWithUriAndKey, string Database, string Container, int CacheMb);

    public class CosmosContext : EquinoxContext
    {
        readonly Cache _cache;

        CosmosStoreContext _store;
        readonly Func<Task> _connect;

        public CosmosContext(CosmosConfig config)
        {
            _cache = new Cache("Cosmos", config.CacheMb);
            var retriesOn429Throttling = 1; // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
            var timeout = TimeSpan.FromSeconds(5); // Timeout applied per request to CosmosDb, including retry attempts
            var discovery = Discovery.NewConnectionString(config.ConnectionStringWithUriAndKey);
            _connect = async () =>
            {
                var connector = new CosmosStoreConnector(discovery, timeout, retriesOn429Throttling, timeout, config.Mode);
                _store = await Connect(connector, config.Database, config.Container);
            };
        }

        internal override async Task Connect() => await _connect();

        static async Task<CosmosStoreContext> Connect(CosmosStoreConnector connector, string databaseId, string containerId)
        {
            var storeClient =
                await FSharpAsync.StartAsTask(
                    CosmosStoreClient.Connect(
                        FuncConvert.FromFunc<(string,string)[], FSharpAsync<CosmosClient>>(connector.CreateAndInitialize),
                        databaseId,
                        containerId),
                    null,
                    null);
            return new CosmosStoreContext(storeClient, tipMaxEvents: 256);
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
                    : AccessStrategy<TEvent, TState>.NewSnapshot(FuncConvert.FromFunc(isOrigin), FuncConvert.FromFunc(toSnapshot));

            var cacheStrategy = _cache == null
                ? null
                : Equinox.CosmosStore.CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
            var cat = new CosmosStoreCategory<TEvent, TState, Unit>(_store, codec.ToJsonElementCodec(), FuncConvert.FromFunc(fold), initial, cacheStrategy, accessStrategy, compressUnfolds:FSharpOption<bool>.None);
            return args => cat.Resolve(handlerLog).Invoke(args.Item1, args.Item2);
        }
    }
}
