module Shipping.Domain.Tests.ShipmentTests

open Shipping.Domain.Shipment

open FsCheck.Xunit
open Swensen.Unquote

// We use Optional string values in the event representations
// However, FsCodec.NewtonsoftJson.OptionConverter maps `Some null` to `None`, which does not roundtrip
// In order to avoid having to special case the assertion in the roundtrip test, we stub out such values
let (|ReplaceSomeNullWithNone|) = TypeShape.Generic.map (function Some (null : string) -> None | x -> x)

let [<Property>] ``events roundtrip`` (ReplaceSomeNullWithNone (x : Events.Event)) =
    let ee = Events.codec.Encode(None, x)
    let e = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    let des = Events.codec.TryDecode e
    test <@ des = Some x @>
