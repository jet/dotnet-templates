namespace SyncTemplate

open Serilog
open Serilog.Events
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

[<System.Runtime.CompilerServices.Extension>]
type Logging() =

    [<System.Runtime.CompilerServices.Extension>]
    static member Configure(configuration : LoggerConfiguration, verbose, changeFeedProcessorVerbose, ?maybeSeqEndpoint) =
        configuration
            .Destructure.FSharpTypes()
            .Enrich.FromLogContext()
        |> fun c -> if verbose then c.MinimumLevel.Debug() else c
        |> fun c -> c.ConfigureChangeFeedProcessorLogging(changeFeedProcessorVerbose)
        |> fun c -> let ingesterLevel = if changeFeedProcessorVerbose then LogEventLevel.Debug else LogEventLevel.Information
                    c.MinimumLevel.Override(typeof<Propulsion.Streams.Scheduling.StreamSchedulingEngine>.FullName, ingesterLevel)
        |> fun c -> let generalLevel = if verbose then LogEventLevel.Information else LogEventLevel.Warning
                    c.MinimumLevel.Override(typeof<Propulsion.Cosmos.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Internal.Writer.Result>.FullName, generalLevel)
                     .MinimumLevel.Override(typeof<Propulsion.EventStore.Checkpoint.CheckpointSeries>.FullName, LogEventLevel.Information)
        |> fun c -> let t = "[{Timestamp:HH:mm:ss} {Level:u3}] {partitionKeyRangeId} {Tranche} {Message:lj} {NewLine}{Exception}"
                    let configure (a : Configuration.LoggerSinkConfiguration) : unit =
                        a.Logger(fun l ->
                            l.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
                             .WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink()) |> ignore) |> ignore
                        a.Logger(fun l ->
                            let isEqx = Filters.Matching.FromSource<Equinox.Cosmos.Core.Context>().Invoke
                            let isWriterA = Filters.Matching.FromSource<Propulsion.EventStore.Internal.Writer.Result>().Invoke
                            let isWriterB = Filters.Matching.FromSource<Propulsion.Cosmos.Internal.Writer.Result>().Invoke
                            let isCp = Filters.Matching.FromSource<Propulsion.EventStore.Checkpoint.CheckpointSeries>().Invoke
                            let l =
                                if changeFeedProcessorVerbose then l
                                else l.Filter.ByExcluding(fun x -> isEqx x || isWriterA x || isWriterB x || isCp x)
                            l.WriteTo.Console(theme=Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code, outputTemplate=t)
                            |> ignore) |> ignore
                    c.WriteTo.Async(bufferSize=65536, blockWhenFull=true, configure=System.Action<_> configure)
        |> fun c -> match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
