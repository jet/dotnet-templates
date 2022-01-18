using Microsoft.FSharp.Core;
using System.Text.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FsCodec.SystemTextJson;

namespace TodoBackendTemplate
{
    public abstract class EquinoxContext
    {
        public abstract Func<string, Equinox.Core.IStream<TEvent, TState>> Resolve<TEvent, TState>(
            FsCodec.IEventCodec<TEvent, byte[], object> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> toSnapshot = null);

        internal abstract Task Connect();
    }

    public static class EquinoxCodec
    {
        public static FsCodec.IEventCodec<TEvent, byte[], object> Create<TEvent>(
            Func<TEvent, Tuple<string, byte[]>> encode,
            Func<string, byte[], TEvent> tryDecode) where TEvent: class
        {
            return FsCodec.Codec.Create(
                FuncConvert.FromFunc(encode),
                FuncConvert.FromFunc((Func<Tuple<string, byte[]>, FSharpOption<TEvent>>) TryDecodeImpl));
            FSharpOption<TEvent> TryDecodeImpl(Tuple<string, byte[]> encoded) => OptionModule.OfObj(tryDecode(encoded.Item1, encoded.Item2));
        }

        public static FsCodec.IEventCodec<TEvent, byte[], object> Create<TEvent>(JsonSerializerOptions options = null) where TEvent: TypeShape.UnionContract.IUnionContract =>
            FsCodec.SystemTextJson.Codec.Create<TEvent>(options).ToByteArrayCodec();
    }
}
