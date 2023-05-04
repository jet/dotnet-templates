using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

namespace TodoBackendTemplate
{
    public abstract class EquinoxContext
    {
        public abstract Func<(string, string), Equinox.DeciderCore<TEvent, TState>> Resolve<TEvent, TState>(
            Serilog.ILogger storeLog,
            FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> toSnapshot = null);

        internal abstract Task Connect();
    }

    public static class EquinoxCodec
    {
        public static FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> Create<TEvent>(
            Func<TEvent, (string, ReadOnlyMemory<byte>)> encode,
            Func<string, ReadOnlyMemory<byte>, FSharpValueOption<TEvent>> tryDecode) where TEvent: class =>
            
            FsCodec.Codec.Create(encode, tryDecode);

        public static FsCodec.IEventCodec<TEvent, ReadOnlyMemory<byte>, Unit> Create<TEvent>(JsonSerializerOptions options = null) where TEvent: TypeShape.UnionContract.IUnionContract =>
            FsCodec.SystemTextJson.Codec.Create<TEvent>(options);
    }
}
