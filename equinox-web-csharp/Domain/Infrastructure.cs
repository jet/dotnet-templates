using Equinox;
using Equinox.Core;
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
        public static void Execute<TEvent, TState>(this Accumulator<TEvent, TState> that, Func<TState, IEnumerable<TEvent>> f) =>
            that.Transact(FuncConvert.FromFunc<TState, FSharpList<TEvent>>(s => ListModule.OfSeq(f(s))));
    }

    public class EquinoxStream<TEvent, TState> : Stream<TEvent, TState>
    {
        private readonly Func<TState, IEnumerable<TEvent>, TState> _fold;

        public EquinoxStream(
                Func<TState, IEnumerable<TEvent>, TState> fold,
                ILogger log, IStream<TEvent, TState> stream, int maxAttempts = 3)
            : base(log, stream, maxAttempts)
        {
            _fold = fold;
        }

        /// Run the decision method, letting it decide whether or not the Command's intent should manifest as Events
        public async Task<Unit> Execute(Func<TState,IEnumerable<TEvent>> interpret)
        {
            FSharpList<TEvent> decide_(TState state)
            {
                var a = new Accumulator<TEvent, TState>(FuncConvert.FromFunc(_fold), state);
                a.Execute(interpret);
                return a.Accumulated;
            }
            return await FSharpAsync.StartAsTask(Transact(FuncConvert.FromFunc<TState,FSharpList<TEvent>>(decide_)), null, null);
        }

        /// Execute a command, as Decide(Action) does, but also yield an outcome from the decision
        public async Task<T> Decide<T>(Func<Accumulator<TEvent, TState>, T> decide)
        {
            Tuple<T, FSharpList<TEvent>> decide_(TState state)
            {
                var a = new Accumulator<TEvent, TState>(FuncConvert.FromFunc(_fold), state);
                var r = decide(a);
                return Tuple.Create(r, a.Accumulated);
            }
            return await FSharpAsync.StartAsTask<T>(Transact(FuncConvert.FromFunc<TState,Tuple<T,FSharpList<TEvent>>>(decide_)), null, null);
        }

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

        public T Decode<T>(byte[] json) where T : class
        {
            using (var ms = new MemoryStream(json))
            using (var jsonReader = new JsonTextReader(new StreamReader(ms)))
                return _serializer.Deserialize<T>(jsonReader);
        }
    }
}