using Equinox;
using Equinox.Store;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Control;
using Microsoft.FSharp.Core;
using Newtonsoft.Json;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

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
        public EquinoxHandler
            (   Func<TState, IEnumerable<TEvent>, TState> fold,
                ILogger log,
                IStream<TEvent, TState> stream,
                int maxAttempts = 3)
            : base(FuncConvert.FromFunc<TState, FSharpList<TEvent>, TState>(fold),
                log,
                stream,
                maxAttempts,
               null,
               null)
        {
        }

        // Run the decision method, letting it decide whether or not the Command's intent should manifest as Events
        public async Task<Unit> Decide(Action<Context<TEvent, TState>> decide) =>
            await FSharpAsync.StartAsTask(Decide(FuncConvert.ToFSharpFunc(decide)), null, null);

        // Execute a command, as Decide(Action) does, but also yield an outcome from the decision
        public async Task<T> Decide<T>(Func<Context<TEvent, TState>,T> interpret) =>
            await FSharpAsync.StartAsTask<T>(Decide(FuncConvert.FromFunc(interpret)), null, null);
        
        // Project from the synchronized state, without the possibility of adding events that Decide(Func) admits
        public async Task<T> Query<T>(Func<TState, T> project) =>
            await FSharpAsync.StartAsTask(Query(FuncConvert.FromFunc(project)), null, null);
    }

    /// Newtonsoft.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
    public class JsonNetUtf8Codec
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