module SyncTemplate.Tests.IngesterTests

open Swensen.Unquote
open SyncTemplate.Program.Ingester
open Xunit

let mk p c : Span = { pos = p; events = [| for x in 0..c-1 -> Equinox.Codec.Core.EventData.Create(p + int64 x |> string, null) |] }
let mergeSpans = Span.merge

let [<Fact>] ``nothing`` () =
    let r = mergeSpans 0L [ mk 0L 0; mk 0L 0 ]
    r =! null

let [<Fact>] ``no overlap`` () =
    let r = mergeSpans 0L [ mk 0L 1; mk 2L 2 ]
    r =! [| mk 0L 1; mk 2L 2 |]

let [<Fact>] ``overlap`` () =
    let r = mergeSpans 0L [ mk 0L 1; mk 0L 2 ]
    r =! [| mk 0L 2 |]

let [<Fact>] ``remove nulls`` () =
    let r = mergeSpans 1L [ mk 0L 1; mk 0L 2 ]
    r =! [| mk 1L 1 |]

let [<Fact>] ``adjacent`` () =
    let r = mergeSpans 0L [ mk 0L 1; mk 1L 2 ]
    r =! [| mk 0L 3 |]

let [<Fact>] ``adjacent trim`` () =
    let r = mergeSpans 1L [ mk 0L 2; mk 2L 2 ]
    r =! [| mk 1L 3 |]

let [<Fact>] ``adjacent trim append`` () =
    let r = mergeSpans 1L [ mk 0L 2; mk 2L 2; mk 5L 1]
    r =! [| mk 1L 3; mk 5L 1 |]

let [<Fact>] ``mixed adjacent trim append`` () =
    let r = mergeSpans 1L [ mk 0L 2; mk 5L 1; mk 2L 2; ]
    r =! [| mk 1L 3; mk 5L 1 |]