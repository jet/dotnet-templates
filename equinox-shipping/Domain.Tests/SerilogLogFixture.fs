namespace Shipping.Domain.Tests

type XunitOutputSink(?messageSink: Xunit.Abstractions.IMessageSink, ?minLevel: Serilog.Events.LogEventLevel, ?templatePrefix) =
    let minLevel = defaultArg minLevel Serilog.Events.LogEventLevel.Information
    let formatter =
        let baseTemplate = "{Timestamp:HH:mm:ss.fff} {Level:u1} " + Option.toObj templatePrefix + "{Message} {Properties}{NewLine}{Exception}"
        let template = if minLevel <= Serilog.Events.LogEventLevel.Debug then baseTemplate else baseTemplate.Replace("{Properties}", "")
        Serilog.Formatting.Display.MessageTemplateTextFormatter(template, null)
    let mutable currentTestOutput: Xunit.Abstractions.ITestOutputHelper option = None
    let writeSerilogEvent (logEvent: Serilog.Events.LogEvent) =
        logEvent.RemovePropertyIfPresent Equinox.CosmosStore.Core.Log.PropertyTag
        logEvent.RemovePropertyIfPresent Equinox.DynamoStore.Core.Log.PropertyTag
        logEvent.RemovePropertyIfPresent Equinox.EventStoreDb.Log.PropertyTag
        logEvent.RemovePropertyIfPresent Propulsion.Streams.Log.PropertyTag
//-:cnd:noEmit
#if !NO_CONCRETE_STORES // In Domain.Tests, we don't reference Propulsion.CosmosStore/DynamoStore etc        
        logEvent.RemovePropertyIfPresent Propulsion.CosmosStore.Log.PropertyTag
        logEvent.RemovePropertyIfPresent Propulsion.Feed.Core.Log.PropertyTag
#endif
//+:cnd:noEmit
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
        let debugging = false // <- set to true to include event Submit/Done info in test run output, even when not running under debugger
        let verbose = false // <- set to true to include extra diagnostics on projector batches etc when debugging
        if System.Diagnostics.Debugger.IsAttached || debugging then
            if verbose then Events.LogEventLevel.Verbose else Events.LogEventLevel.Debug
        else Events.LogEventLevel.Information
#else
        Events.LogEventLevel.Debug // we want details from CI failures
#endif

    let createForSink sink =
        LoggerConfiguration()
            .Enrich.FromLogContext()
            .WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
            .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
            .WriteTo.Sink(Equinox.EventStoreDb.Log.InternalMetrics.Stats.LogSink())
            .MinimumLevel.Is(minLevel)
            .WriteTo.Logger(fun l ->
                l.Filter.ByExcluding(Store.Metrics.logEventIsMetric) // <- comment out to see Equinox logs in Test Output
                 .WriteTo.Sink(sink) |> ignore)
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
type SerilogLogFixture(messageSink, ?templatePrefix) =
    let sink = XunitOutputSink(messageSink, XunitLogger.minLevel, ?templatePrefix = templatePrefix)
    do Serilog.Log.Logger <- XunitLogger.createForSink sink; Serilog.Log.Information "Serilog.Log -> Xunit configured..."
    /// Enables capturing of log messages in the per-test output (combine with XUnit Collection fixtures to ensure no concurrent usage)
    member _.CaptureSerilogLog value = sink.CaptureSerilogLog value
    member _.TestOutput value = sink.CaptureSerilogLog value;
    interface System.IDisposable with member _.Dispose() = Serilog.Log.Information "... Serilog.Log -> Xunit removed"; Serilog.Log.CloseAndFlush()
