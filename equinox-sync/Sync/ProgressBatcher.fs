module SyncTemplate.ProgressBatcher

open System.Collections.Generic

type [<NoComparison>] internal Chunk<'Pos> = { pos: 'Pos; streamToRequiredIndex : Dictionary<string,int64> }

type State<'Pos>(?currentPos : 'Pos) =
    let pending = Queue<_>()
    let mutable validatedPos = currentPos
    member __.AppendBatch(pos, streamWithRequiredIndices : (string * int64) seq) =
        let byStream = streamWithRequiredIndices |> Seq.groupBy fst |> Seq.map (fun (s,xs) -> KeyValuePair(s,xs |> Seq.map snd |> Seq.max))
        pending.Enqueue { pos = pos; streamToRequiredIndex = Dictionary byStream }
    member __.MarkStreamProgress(stream, index) =
        for x in pending do
            match x.streamToRequiredIndex.TryGetValue stream with
            | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
            | _, _ -> ()
        let headIsComplete () =
            match pending.TryPeek() with
            | true, batch -> batch.streamToRequiredIndex.Count = 0
            | _ -> false
        while headIsComplete () do
            let headBatch = pending.Dequeue()
            validatedPos <- Some headBatch.pos
    member __.Validate tryGetStreamWritePos : 'Pos option * int =
        let rec aux () =
            match pending.TryPeek() with
            | false, _ -> ()
            | true, batch ->
                for KeyValue (stream, requiredIndex) in Array.ofSeq batch.streamToRequiredIndex do
                    //Log.Warning("VI {s} {ri}", stream, requiredIndex)
                    match tryGetStreamWritePos stream with
                    | Some index when requiredIndex <= index -> batch.streamToRequiredIndex.Remove stream |> ignore
                    | _ -> ()
                if batch.streamToRequiredIndex.Count = 0 then
                    let headBatch = pending.Dequeue()
                    validatedPos <- Some headBatch.pos
                    aux ()
        aux ()
        validatedPos, pending.Count