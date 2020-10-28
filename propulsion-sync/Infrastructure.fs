namespace SyncTemplate

open Serilog
open System.Runtime.CompilerServices

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
