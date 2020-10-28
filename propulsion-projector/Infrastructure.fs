[<AutoOpen>]
module ProjectorTemplate.Infrastructure

open Serilog
open System.Runtime.CompilerServices

#if cosmos
[<Extension>]
type LoggerConfigurationExtensions() =

    [<Extension>]
    static member inline ExcludeChangeFeedProcessorV2InternalDiagnostics(c : LoggerConfiguration) =
        let isCfp429a = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement.DocumentServiceLeaseUpdater").Invoke
        let isCfp429b = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.LeaseRenewer").Invoke
        let isCfp429c = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement.PartitionLoadBalancer").Invoke
        let isCfp429d = Filters.Matching.FromSource("Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.PartitionProcessor").Invoke
        let isCfp x = isCfp429a x || isCfp429b x || isCfp429c x || isCfp429d x
        c.Filter.ByExcluding(fun x -> isCfp x)

    [<Extension>]
    static member inline ConfigureChangeFeedProcessorLogging(c : LoggerConfiguration, verbose : bool) =
        // LibLog writes to the global logger, so we need to control the emission
        let cfpl = if verbose then Serilog.Events.LogEventLevel.Debug else Serilog.Events.LogEventLevel.Warning
        c.MinimumLevel.Override("Microsoft.Azure.Documents.ChangeFeedProcessor", cfpl)
        |> fun c -> if verbose then c else c.ExcludeChangeFeedProcessorV2InternalDiagnostics()

#endif
// Application logic assumes the global `Serilog.Log` is initialized _immediately_ after a successful ArgumentParser.ParseCommandline
type Logging() =

    static member Initialize(configure) =
        let loggerConfiguration : LoggerConfiguration = LoggerConfiguration() |> configure
        Log.Logger <- loggerConfiguration.CreateLogger()

#if cosmos
    static member Configure(configuration : LoggerConfiguration, ?minimumLevel, ?changeFeedProcessorVerbose) =
#else
    static member Configure(configuration : LoggerConfiguration, ?verbose) =
#endif
            configuration
                .Destructure.FSharpTypes()
                .Enrich.FromLogContext()
            |> fun c -> if verbose = Some true then c.MinimumLevel.Debug() else c
#if cosmos
            |> fun c -> c.ConfigureChangeFeedProcessorLogging((changeFeedProcessorVerbose = Some true))
#endif
            |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId,2} {Message:lj} {NewLine}{Exception}"
                        c.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
