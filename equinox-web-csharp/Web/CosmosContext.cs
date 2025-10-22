using Equinox;
using Equinox.CosmosStore;
using FsCodec.SystemTextJson.Interop;
using Microsoft.Azure.Cosmos;
using Microsoft.FSharp.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TodoBackendTemplate;

public record CosmosConfig(ConnectionMode Mode, string ConnectionStringWithUriAndKey, string Database, string Container, int CacheMb);

public class CosmosContext : EquinoxContext
{
    readonly Cache _cache;

    CosmosStoreContext _context;
    readonly Func<Task> _connect;

    public CosmosContext(CosmosConfig config)
    {
        _cache = new Cache("Cosmos", config.CacheMb);
        var retriesOn429Throttling = 1; // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
        var timeout = TimeSpan.FromSeconds(5); // Timeout applied per request to CosmosDb, including retry attempts
        var discovery = Discovery.NewConnectionString(config.ConnectionStringWithUriAndKey);
        _connect = async () =>
        {
            var connector = new CosmosStoreConnector(discovery, retriesOn429Throttling, timeout, config.Mode);
            _context = await Connect(connector, config.Database, config.Container);
        };
    }

    internal override async Task Connect() => await _connect();

    static async Task<CosmosStoreContext> Connect(CosmosStoreConnector connector, string databaseId, string containerId)
    {
        var client = await connector.ConnectAsync(new [] {(databaseId,containerId)}, new CancellationToken());
        return new CosmosStoreContext(client, databaseId, containerId, tipMaxEvents: 256);
    }

    public override Func<string, DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
        string name,
        Serilog.ILogger handlerLog,
        FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
        Func<TState, TEvent[], TState> fold,
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
            : CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
        var cat = new CosmosStoreCategory<TEvent, TState, Unit>(_context, name, FsCodec.SystemTextJson.Encoder.CompressedUtf8(codec), fold, initial, accessStrategy, cacheStrategy);
        return cat.Resolve(handlerLog);
    }
}