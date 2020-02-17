module Fc.LocationEpochTests

open FsCheck.Xunit
open Location.Epoch
open Swensen.Unquote

let interpret transactionId delta _balance =
    match delta with
    | 0 -> (), []
    | delta when delta < 0 -> (), [Events.Removed { delta = -delta; transaction = transactionId }]
    | delta -> (), [Events.Added { delta = delta; transaction = transactionId }]

let validateAndInterpret transactionId expectedBalance delta balance =
    test <@ expectedBalance = balance @>
    interpret transactionId delta balance

let verifyDeltaEvent transactionId delta events =
    let dEvents = events |> List.filter (function Events.Added _ | Events.Removed _ -> true | _ -> false)
    test <@ interpret transactionId delta (Unchecked.defaultof<_>) = ((), dEvents) @>

let [<Property>] properties transactionId carriedForward delta1 closeImmediately delta2 close =

    (* Starting with an empty stream, we'll need to supply the balance carried forward, optionally we apply a delta and potentially close *)

    let initialShouldClose _state = closeImmediately
    let res, events =
        sync (Some carriedForward) (validateAndInterpret transactionId carriedForward delta1) initialShouldClose Fold.initial
    let cfEvents events = events |> List.filter (function Events.CarriedForward _ -> true | _ -> false)
    let closeEvents events = events |> List.filter (function Events.Closed -> true | _ -> false)
    let state1 = Fold.fold Fold.initial events
    let expectedBalance = carriedForward + delta1
    // Only expect closing if it was requested
    let expectImmediateClose = closeImmediately
    test <@ Option.isSome res.result
            && expectedBalance = res.balance @>
    test <@ [Events.CarriedForward { initial = carriedForward }] = cfEvents events
            && (not expectImmediateClose || 1 = Seq.length (closeEvents events)) @>
    verifyDeltaEvent transactionId delta1 events

    (* After initializing, validate we don't need to supply a carriedForward, and don't produce a CarriedForward event *)

    let shouldClose _state = close
    let { isOpen = isOpen; result = worked; balance = bal }, events =
        sync None (validateAndInterpret transactionId expectedBalance delta2) shouldClose state1
    let expectedBalance = if expectImmediateClose then expectedBalance else expectedBalance + delta2
    test <@ [] = cfEvents events
            && (expectImmediateClose || not close || 1 = Seq.length (closeEvents events)) @>
    test <@ (expectImmediateClose || close || isOpen)
            && expectedBalance = bal @>
    if not expectImmediateClose then
        test <@ Option.isSome worked @>
        verifyDeltaEvent transactionId delta2 events

let [<Property>] ``codec can roundtrip`` event =
    let ee = Events.codec.Encode(None, event)
    let ie = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    test <@ Some event = Events.codec.TryDecode ie @>
