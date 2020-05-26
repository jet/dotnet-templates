module Shipping.Domain.Tests.ContainerTests

open Shipping.Domain.Container

open FsCheck.Xunit
open Swensen.Unquote

let [<Property>] ``events roundtrip`` (x : Events.Event) =
    let ee = Events.codec.Encode(None, x)
    let e = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    let des = Events.codec.TryDecode e
    test <@ des = Some x @>