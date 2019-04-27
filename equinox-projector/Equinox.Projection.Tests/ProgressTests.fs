module ProgressTests

open Equinox.Projection.State

open Swensen.Unquote
open Xunit
open System.Collections.Generic

let mkDictionary xs = Dictionary<string,int64>(dict xs)

let [<Fact>] ``Empty has zero streams pending or progress to write`` () =
    let sut = ProgressState<_>()
    let completed,validatedPos, batches = sut.Validate(fun _ -> None)
    0 =! completed
    None =! validatedPos
    0 =! batches

let [<Fact>] ``Can add multiple batches`` () =
    let sut = ProgressState<_>()
    sut.AppendBatch(0,mkDictionary ["a",1L; "b",2L])
    sut.AppendBatch(1,mkDictionary ["b",2L; "c",3L])
    let completed,validatedPos, batches = sut.Validate(fun _ -> None)
    0 =! completed
    None =! validatedPos
    2 =! batches

let [<Fact>] ``Marking Progress Removes batches and updates progress`` () =
    let sut = ProgressState<_>()
    sut.AppendBatch(0,mkDictionary ["a",1L; "b",2L])
    sut.MarkStreamProgress("a",1L) |> ignore
    sut.MarkStreamProgress("b",1L) |> ignore
    let completed, validatedPos, batches = sut.Validate(fun _ -> None)
    0 =! completed
    None =! validatedPos
    1 =! batches

let [<Fact>] ``Marking progress is not persistent`` () =
    let sut = ProgressState<_>()
    sut.AppendBatch(0, mkDictionary ["a",1L])
    sut.MarkStreamProgress("a",2L) |> ignore
    sut.AppendBatch(1, mkDictionary ["a",1L; "b",2L])
    let completed, validatedPos, batches = sut.Validate(fun _ -> None)
    0 =! completed
    Some 0 =! validatedPos
    1 =! batches