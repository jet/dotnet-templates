module SyncTemplate.Tests.CosmosIngesterTests

open SyncTemplate.Program.CosmosIngester
open Swensen.Unquote
open Xunit

let canonicalTime = System.DateTimeOffset.UtcNow
let mk p c : Span = { index = p; events = [| for x in 0..c-1 -> Equinox.Codec.Core.EventData.Create(p + int64 x |> string, null, timestamp=canonicalTime) |] }
let mergeSpans = Queue.Span.merge

let [<Fact>] ``nothing`` () =
    let r = mergeSpans 0L [ mk 0L 0; mk 0L 0 ]
    r =! null

let [<Fact>] ``synced`` () =
    let r = mergeSpans 1L [ mk 0L 1; mk 0L 0 ]
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