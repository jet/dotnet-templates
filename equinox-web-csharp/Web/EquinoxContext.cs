using Microsoft.FSharp.Core;
using System.Text.Json;

namespace TodoBackendTemplate;

public abstract class EquinoxContext
{
    public abstract Func<string, Equinox.DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
        string name,
        Serilog.ILogger storeLog,
        FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
        Func<TState, TEvent[], TState> fold,
        TState initial,
        Func<TEvent, bool>? isOrigin = null,
        Func<TState, TEvent>? toSnapshot = null);

    internal abstract Task Connect();
}

public static class EquinoxCodec
{
    public static FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> Create<TEvent>(
        Func<TEvent, (string, ReadOnlyMemory<byte>)> encode,
        Func<string, ReadOnlyMemory<byte>, TEvent?> tryDecode) =>
        FsCodec.Codec.Create(encode, (s, b) => tryDecode(s,b) ?? FSharpValueOption<TEvent>.None);
    public static FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> Create<TEvent>(JsonSerializerOptions? options = null) where TEvent : TypeShape.UnionContract.IUnionContract =>
        FsCodec.SystemTextJson.Codec.Create<TEvent>(options);
}