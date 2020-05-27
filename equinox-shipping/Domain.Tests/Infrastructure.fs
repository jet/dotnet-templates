[<AutoOpen>]
module Shipping.Domain.Tests.Infrastructure

open System.Collections.Concurrent

type EventAccumulator<'E>() =
    let messages = ConcurrentDictionary<FsCodec.StreamName, ConcurrentQueue<'E>>()

    member __.Record(stream, events : 'E seq) =
        let initStreamQueue _ = ConcurrentQueue events
        let appendToQueue _ (queue : ConcurrentQueue<'E>) = events |> Seq.iter queue.Enqueue; queue
        messages.AddOrUpdate(stream, initStreamQueue, appendToQueue) |> ignore

    member __.Queue stream =
        match messages.TryGetValue stream with
        | false, _ -> Seq.empty<'E>
        | true, xs -> xs :> _

    member __.All() = seq { for KeyValue (_, xs) in messages do yield! xs }

    member __.Clear() =
        messages.Clear()

type Async with

    /// Returns an async computation which runs the argument computation but raises an exception if it doesn't complete
    /// by the specified timeout.
    static member timeoutAfter (timeout : System.TimeSpan) (c : Async<'a>) = async {
        let! r = Async.StartChild(c, int timeout.TotalMilliseconds)
        return! r }

(* Generic FsCheck helpers *)
let (|Id|) (x : System.Guid) = x.ToString "N" |> FSharp.UMX.UMX.tag
let (|Ids|) (xs : System.Guid[]) = xs |> Array.map (|Id|)
let (|IdsMoreThanOne|) (Ids xs, Id x) = [| yield x; yield! xs |]
let (|AtLeastOne|) (x, xs) = x::xs

type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer);
        writer |> string |> testOutput.WriteLine
        writer |> string |> System.Diagnostics.Debug.Write
    interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent
