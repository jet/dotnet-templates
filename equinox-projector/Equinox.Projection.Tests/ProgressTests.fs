module ProgressTests

open Equinox.Projection.State
open Swensen.Unquote
open System.Collections.Generic
open Xunit

let mkDictionary xs = Dictionary<string,int64>(dict xs)

let [<Fact>] ``Empty has zero streams pending or progress to write`` () =
    let sut = ProgressState<_>()
    let completed = sut.Validate(fun _ -> None)
    0 =! completed

let [<Fact>] ``Can add multiple batches`` () =
    let sut = ProgressState<_>()
    let noBatchesComplete () = failwith "No bathes should complete"
    sut.AppendBatch(noBatchesComplete, mkDictionary ["a",1L; "b",2L])
    sut.AppendBatch(noBatchesComplete, mkDictionary ["b",2L; "c",3L])
    let completed = sut.Validate(fun _ -> None)
    0 =! completed

let [<Fact>] ``Marking Progress Removes batches and updates progress`` () =
    let sut = ProgressState<_>()
    let callbacks = ref 0
    let complete () = incr callbacks
    sut.AppendBatch(complete, mkDictionary ["a",1L; "b",2L])
    sut.MarkStreamProgress("a",1L) |> ignore
    sut.MarkStreamProgress("b",1L) |> ignore
    let completed = sut.Validate(fun _ -> None)
    0 =! completed
    1 =! !callbacks

let [<Fact>] ``Marking progress is not persistent`` () =
    let sut = ProgressState<_>()
    let callbacks = ref 0
    let complete () = incr callbacks
    sut.AppendBatch(complete, mkDictionary ["a",1L])
    sut.MarkStreamProgress("a",2L) |> ignore
    sut.AppendBatch(complete, mkDictionary ["a",1L; "b",2L])
    let completed = sut.Validate(fun _ -> None)
    0 =! completed
    1 =! !callbacks