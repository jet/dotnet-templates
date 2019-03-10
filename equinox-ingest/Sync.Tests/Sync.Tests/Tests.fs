module SyncTemplate.Tests.IngesterTests

open Swensen.Unquote
open SyncTemplate.Program.Ingester
open Xunit

//type Span = { pos: int64; events: string[] }

//let (|Max|) x = x.pos + x.events.LongLength
//let trim min (Max m as x) =
//    // Full remove
//    if m <= min then { pos = min; events = [||] }
//    // Trim until min
//    elif m > min && x.pos < min then { pos = min; events = x.events |> Array.skip (min - x.pos |> int) }
//    // Leave it
//    else x
//let mergeSpans min (xs : Span seq) =
//    let buffer = ResizeArray()
//    let mutable curr = { pos = min; events = [||]}
//    for x in xs |> Seq.sortBy (fun s -> s.pos) do
//        match curr, trim min x with
//        // no data incoming, skip
//        | _, x when x.events.Length = 0 ->
//            ()
//        // Not overlapping, no data buffered -> buffer
//        | c, x when c.events.Length = 0 ->
//            curr <- x
//        // Overlapping, join
//        | Max cMax as c, x when cMax >= x.pos ->
//            curr <- { c with events = Array.append c.events (trim cMax x).events }
//        // Not overlapping, new data
//        | c, x ->
//            buffer.Add c
//            curr <- x
//    if curr.events.Length <> 0 then buffer.Add curr
//    if buffer.Count = 0 then null else buffer.ToArray()

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