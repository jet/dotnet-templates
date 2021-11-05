namespace Shipping.Watchdog.Integration

type XunitOutputSink(?messageSink : Xunit.Abstractions.IMessageSink, ?minLevel : Serilog.Events.LogEventLevel) =
    let minLevel = defaultArg minLevel Serilog.Events.LogEventLevel.Information
    let formatter =
        let baseTemplate = "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u1} {Message} {Properties}{NewLine}{Exception}"
        let template = if minLevel <= Serilog.Events.LogEventLevel.Verbose then baseTemplate else baseTemplate.Replace("{Properties}", "")
        Serilog.Formatting.Display.MessageTemplateTextFormatter(template, null)
    let mutable currentTestOutput : Xunit.Abstractions.ITestOutputHelper option = None
    let writeSerilogEvent (logEvent : Serilog.Events.LogEvent) =
        logEvent.RemovePropertyIfPresent Propulsion.Streams.Log.PropertyTag
        logEvent.RemovePropertyIfPresent Propulsion.CosmosStore.Log.PropertyTag
        logEvent.RemovePropertyIfPresent "cosmosEvt" // TOOD add constant to Equinox
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        let message = writer |> string |> fun s -> s.TrimEnd('\n')
        currentTestOutput |> Option.iter (fun testOutput -> testOutput.WriteLine message)
        messageSink |> Option.iter (fun sink -> sink.OnMessage(Xunit.Sdk.DiagnosticMessage message) |> ignore)
        //writer |> string |> System.Diagnostics.Debug.Write
    member _.CaptureSerilogLog(testOutput) =
        currentTestOutput <- Some testOutput
        { new System.IDisposable with member _.Dispose() = currentTestOutput <- None }
    interface Serilog.Core.ILogEventSink with member _.Emit logEvent = writeSerilogEvent logEvent

module XunitLogger =

    open Serilog

    let minLevel =
#if DEBUG
        let debugEvents = false // <- set to true to include event Submit/Done info in test run output
        if System.Diagnostics.Debugger.IsAttached then Events.LogEventLevel.Verbose
        elif debugEvents then Events.LogEventLevel.Debug
        else Events.LogEventLevel.Information
#else
        Events.LogEventLevel.Debug // we want details from CI failures
#endif

    let createForSink sink =
        LoggerConfiguration()
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
            .Filter.ByExcluding(System.Func<_, _> Shipping.Watchdog.Infrastructure.Log.isStoreMetrics) // <- comment out to see Equinox logs
            .MinimumLevel.Is(minLevel)
            .WriteTo.Sink(sink)
            .CreateLogger()

    /// Creates a logger that the test will use to surface all messages
    /// NOTE The static Serilog.Log is not configured; see SerilogFixture for support of that
    let forTest testOutput =
        let sink = XunitOutputSink(minLevel = minLevel)
        sink.CaptureSerilogLog testOutput |> ignore // Don't burden the caller with unsubscribing (/trust nobody will attempt to log after the test)
        createForSink sink

/// Ensures we have an initialized global Serilog.Log.Logger, that propagates to the Xunit MessageSink
/// In order to access the full output:
/// 0. have an xunit.runner.json alongside the test assembly with `{ "diagnosticMessages": true }` within
/// 1. Run tests with dotnet test, or Visual Studio and examine Test Output window
type SerilogLogFixture(messageSink) =
    let sink = XunitOutputSink(messageSink, XunitLogger.minLevel)
    do Serilog.Log.Logger <- XunitLogger.createForSink sink; Serilog.Log.Information "Serilog.Log -> Xunit configured..."
    /// Enables capturing of log messages in the per-test output (combine with XUnit Collection fixtures to ensure no concurrent usage)
    member _.CaptureSerilogLog value = sink.CaptureSerilogLog value
    interface System.IDisposable with member _.Dispose() = Serilog.Log.Information "... Serilog.Log -> Xunit removed"; Serilog.Log.CloseAndFlush()
