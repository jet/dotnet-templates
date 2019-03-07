using Equinox;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TypeShape;

namespace TodoBackendTemplate
{
    public abstract class EquinoxContext
    {
        public abstract Func<Target,Equinox.Store.IStream<TEvent, TState>> Resolve<TEvent, TState>(
            Equinox.Codec.IUnionEncoder<TEvent, byte[]> codec,
            Func<TState, IEnumerable<TEvent>, TState> fold,
            TState initial,
            Func<TEvent, bool> isOrigin = null,
            Func<TState, TEvent> compact = null);

        internal abstract Task Connect();
    }

    public static class EquinoxCodec
    {
        static readonly JsonSerializerSettings _defaultSerializationSettings = new Newtonsoft.Json.JsonSerializerSettings();
        
        public static Equinox.Codec.IUnionEncoder<TEvent, byte[]> Create<TEvent>(
            Func<TEvent, Tuple<string,byte[]>> encode,
            Func<string, byte[], TEvent> tryDecode,
            JsonSerializerSettings settings = null) where TEvent: class
        {
            return Equinox.Codec.JsonUtf8.Create<TEvent>(
                FuncConvert.FromFunc(encode),
                FuncConvert.FromFunc((Func<Tuple<string, byte[]>, FSharpOption<TEvent>>) TryDecodeImpl));
            FSharpOption<TEvent> TryDecodeImpl(Tuple<string, byte[]> encoded) => OptionModule.OfObj(tryDecode(encoded.Item1, encoded.Item2));
        }

        public static Equinox.Codec.IUnionEncoder<TEvent, byte[]> Create<TEvent>(JsonSerializerSettings settings = null) where TEvent: UnionContract.IUnionContract =>
            Equinox.Codec.JsonNet.JsonUtf8.Create<TEvent>(settings ?? _defaultSerializationSettings);
    } 
}