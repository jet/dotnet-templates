module ProgressTests

open SyncTemplate.Program

open Swensen.Unquote
open Xunit

let [<Fact>] ``Empty has zero streams pending or progress to write`` () =
    let sut = Progress.State<_>()
    let validatedPos, batches = sut.Validate(fun _ -> None)
    None =! validatedPos
    0 =! batches

let [<Fact>] ``Can add multiple batches`` () =
    let sut = Progress.State<_>()
    sut.AppendBatch(0,["a",1L; "b",2L])
    sut.AppendBatch(1,["b",2L; "c",3L])
    let validatedPos, batches = sut.Validate(fun _ -> None)
    None =! validatedPos
    2 =! batches

let [<Fact>] ``Marking Progress Removes batches and updates progress`` () =
    let sut = Progress.State<_>()
    sut.AppendBatch(0,["a",1L; "b",2L])
    sut.MarkStreamProgress("a",1L)
    sut.MarkStreamProgress("b",1L)
    let validatedPos, batches = sut.Validate(fun _ -> None)
    None =! validatedPos
    1 =! batches

let [<Fact>] ``Marking progress is not persistent`` () =
    let sut = Progress.State<_>()
    sut.AppendBatch(0,["a",1L])
    sut.MarkStreamProgress("a",2L)
    sut.AppendBatch(1,["a",1L; "b",2L])
    let validatedPos, batches = sut.Validate(fun _ -> None)
    Some 0 =! validatedPos
    1 =! batches