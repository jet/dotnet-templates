module Fc.LocationSeriesTests

open FsCheck.Xunit
open FSharp.UMX
open Swensen.Unquote
open Location.Series

let [<Property>] properties c1 c2 =
    let events = interpretAdvanceIngestionEpoch c1 Fold.initial
    let state1 = Fold.fold Fold.initial events
    let epoch0 = %0
    match c1, events, state1 with
    // Started events are not written for < 0
    | n, [], activeEpoch when n < epoch0 ->
        test <@ None = activeEpoch @>
    // Any >=0 value should trigger a Started event, initially
    | n, [Events.Started { epoch = ee }], Some activatedEpoch ->
        test <@ n >= epoch0 && n = ee && n = activatedEpoch @>
    // Nothing else should yield events
    | _, l, _ ->
        test <@ List.isEmpty l @>

    let events = interpretAdvanceIngestionEpoch c2 state1
    let state2 = Fold.fold state1 events
    match state1, c2, events, state2 with
    // Started events are not written for < 0
    | None, n, [], activeEpoch when n < epoch0 ->
        test <@ None = activeEpoch @>
    // Any >= 0 epochId should trigger a Started event if first command didnt do anything
    | None, n, [Events.Started { epoch = ee }], Some activatedEpoch ->
        let eEpoch = %ee
        test <@ n >= epoch0 && n = eEpoch && n = activatedEpoch @>
    // Any higher epochId should trigger a Started event (gaps are fine - we are only tying to reduce walks)
    | Some s1, n, [Events.Started { epoch = ee }], Some activatedEpoch ->
        let eEpoch = %ee
        test <@ n > s1 && n = eEpoch && n > epoch0 && n = activatedEpoch @>
    // Nothing else should yield events
    | _, _, l, _ ->
        test <@ List.isEmpty l @>

let [<Property>] ``codec can roundtrip`` event =
    let ee = Events.codec.Encode(None, event)
    let ie = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    test <@ Some event = Events.codec.TryDecode ie @>
