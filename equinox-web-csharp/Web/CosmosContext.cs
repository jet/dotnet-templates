using Equinox;
using Equinox.CosmosStore;
using FsCodec.SystemTextJson.Interop;
using Microsoft.Azure.Cosmos;
using Microsoft.FSharp.Core;

namespace TodoBackendTemplate;

public record CosmosConfig(ConnectionMode Mode, string ConnectionStringWithUriAndKey, string Database, string Container, int CacheMb);

public class CosmosContext(CosmosConfig config) : EquinoxContext
{
    readonly Cache _cache = new("Cosmos", config.CacheMb);
    CosmosStoreContext _storeContext = null!;

    internal override async Task Connect()
    {
        const int retriesOn429Throttling = 1; // Number of retries before failing processing when provisioned RU/s limit in CosmosDb is breached
        var timeout = TimeSpan.FromSeconds(5); // Timeout applied per request to CosmosDb, including retry attempts
        var discovery = Discovery.NewConnectionString(config.ConnectionStringWithUriAndKey);
        var connector = new CosmosStoreConnector(discovery, retriesOn429Throttling, timeout, config.Mode);
        var client = await connector.ConnectAsync([(config.Database, config.Container)], new CancellationToken());
        _storeContext = new CosmosStoreContext(client, config.Database, config.Container, tipMaxEvents: 256);
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
                : AccessStrategy<TEvent, TState>.NewSnapshot(FuncConvert.FromFunc(isOrigin!), FuncConvert.FromFunc(toSnapshot!));
        var cacheStrategy = CachingStrategy.NewSlidingWindow(_cache, TimeSpan.FromMinutes(20));
        var cat = new CosmosStoreCategory<TEvent, TState, Unit>(_storeContext, name, FsCodec.SystemTextJson.Encoder.CompressedUtf8(codec), fold, initial, accessStrategy, cacheStrategy);
        return cat.Resolve(handlerLog);
    }
}