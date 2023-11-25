module Shipping.Domain.Tests.ShipmentTests

open Shipping.Domain.Shipment

open FsCheck.Xunit
open Swensen.Unquote

let [<Property>] ``events roundtrip`` (x: Events.Event) =
    let ee = Events.codec.Encode((), x)
    let e = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    let des = Events.codec.Decode e
    test <@ des = ValueSome x @>
