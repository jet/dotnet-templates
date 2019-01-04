using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Equinox;
using Equinox.Store;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using Serilog;

namespace TodoBackendTemplate
{
    public static class HandlerExtensions
    {
        public static void Execute<TEvent, TState>(this Context<TEvent, TState> that,
            Func<TState, IEnumerable<TEvent>> f) =>
            that.Execute(FuncConvert.FromFunc<TState, FSharpList<TEvent>>(s => ListModule.OfSeq(f(s))));
    }

    public class EquinoxHandler<TEvent, TState> : Handler<TEvent, TState>
    {
        public EquinoxHandler(Func<TState, IEnumerable<TEvent>, TState> fold, ILogger log,
            IStream<TEvent, TState> stream)
            : base(FuncConvert.FromFunc<TState, FSharpList<TEvent>, TState>(fold), log, stream, 3, null, null)
        {
        }

        public async Task<Unit> Decide(Action<Context<TEvent, TState>> f) =>
            await FSharpAsync.StartAsTask(Decide(FuncConvert.ToFSharpFunc(f)), null, null);

        public async Task<T> Query<T>(Func<TState, T> projection) =>
            await FSharpAsync.StartAsTask(Query(FuncConvert.FromFunc(projection)), null, null);
    }

    /// Newtonsoft.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
    class JsonNetUtf8Codec
    {
        readonly JsonSerializer _serializer;

        public JsonNetUtf8Codec(JsonSerializerSettings settings) =>
            _serializer = JsonSerializer.Create(settings);

        public byte[] Encode<T>(T value) where T : class
        {
            using (var ms = new MemoryStream())
            {
                using (var jsonWriter = new JsonTextWriter(new StreamWriter(ms)))
                    _serializer.Serialize(jsonWriter, value, typeof(T));
                return ms.ToArray();
            }
        }

        public T Decode<T>(byte[] json) where T: class
        {
            using (var ms = new MemoryStream(json))
            using (var jsonReader = new JsonTextReader(new StreamReader(ms)))
               return _serializer.Deserialize<T>(jsonReader);
        }
    }
    
}