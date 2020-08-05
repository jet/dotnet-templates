[<AutoOpen>]
module Fc.Domain.Tests.Fixtures

open Serilog
open System

module EnvVar =

    let tryGet k = Environment.GetEnvironmentVariable k |> Option.ofObj

module EventStore =

    open Equinox.EventStore
    let connect () =
        match EnvVar.tryGet "EQUINOX_ES_HOST", EnvVar.tryGet "EQUINOX_ES_USERNAME", EnvVar.tryGet "EQUINOX_ES_PASSWORD" with
        | Some h, Some u, Some p ->
            let appName = "Domain.Tests"
            let discovery = Discovery.GossipDns h
            let connector = Connector(u, p, TimeSpan.FromSeconds 5., 5, Logger.SerilogNormal Serilog.Log.Logger)
            let connection = connector.Establish(appName, discovery, ConnectionStrategy.ClusterSingle NodePreference.Master) |> Async.RunSynchronously
            let context = Context(connection, BatchingPolicy(500))
            let cache = Equinox.Cache (appName, 10)
            context, cache
        | h, u, p ->
            failwithf "Host, Username and Password EQUINOX_ES_* Environment variables are required (%b,%b,%b)"
                (Option.isSome h) (Option.isSome u) (Option.isSome p)

module TestOutputLogger =

    /// Adapts the XUnit ITestOutputHelper to be a Serilog Sink
    type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
        let template = "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message} {Properties}{NewLine}{Exception}"
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter(template, null);
        let writeSerilogEvent logEvent =
            use writer = new System.IO.StringWriter()
            formatter.Format(logEvent, writer)
            let messageLine = string writer
            testOutput.WriteLine messageLine
            System.Diagnostics.Debug.Write messageLine
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

    let create output =
        let logger = TestOutputAdapter output
        LoggerConfiguration().Destructure.FSharpTypes().WriteTo.Sink(logger).CreateLogger()

(* Generic FsCheck helpers *)

let (|Id|) (x : Guid) = x.ToString "N" |> FSharp.UMX.UMX.tag
let inline mkId () = Guid.NewGuid() |> (|Id|)
let (|Ids|) (xs : Guid[]) = xs |> Array.map (|Id|)
let (|IdsAtLeastOne|) (Ids xs, Id x) = [| yield x; yield! xs |]
let (|AtLeastOne|) (x, xs) = x::xs

