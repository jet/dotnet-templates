namespace App

open FSharp.Control
open Propulsion.Feed
open System

/// <summary>Parses CR separated file with items dumped from a Cosmos Container containing Equinox Items</summary>
/// <remarks>One way to generate one of those is via the cosmic tool at https://github.com/creyke/Cosmic
///   dotnet tool install -g cosmic
///   # then connect/select db per https://github.com/creyke/Cosmic#basic-usage
///   cosmic query 'select * from c order by c._ts' > file.out </remarks>
type [<Sealed; AbstractClass>] CosmosDumpSource private () =

    static member Start(log, statsInterval, filePath, skip, parseFeedDoc, sink, ?truncateTo) =
        let isNonCommentLine line = System.Text.RegularExpressions.Regex.IsMatch(line, "^\s*#") |> not
        let truncate = match truncateTo with Some count -> Seq.truncate count | None -> id
        let lines = Seq.append (System.IO.File.ReadLines filePath |> truncate) (Seq.singleton null) // Add a trailing EOF sentinel so checkpoint positions can be line numbers even when finished reading
        let crawl _ _ _ = taskSeq {
            for i, line in lines |> Seq.indexed do
                let isEof = line = null
                if isEof || (i >= skip && isNonCommentLine line) then
                    let lineNo = int64 i + 1L
                    try let items = if isEof then Array.empty
                                    else System.Text.Json.JsonDocument.Parse line |> parseFeedDoc |> Seq.toArray
                        struct (TimeSpan.Zero, ({ items = items; isTail = isEof; checkpoint = Position.parse lineNo }: Core.Batch<_>))
                    with e -> raise <| exn($"File Parse error on L{lineNo}: '{line.Substring(0, 200)}'", e) }
        let source =
            let checkpointStore = Equinox.MemoryStore.VolatileStore()
            let checkpoints = ReaderCheckpoint.MemoryStore.create log ("consumerGroup", TimeSpan.FromMinutes 1) checkpointStore
            Propulsion.Feed.Core.SinglePassFeedSource(log, statsInterval, SourceId.parse filePath, crawl, checkpoints, sink, string)
        source.Start(fun _ct -> task { return [| TrancheId.parse "0" |] })
