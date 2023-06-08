/// Integration suite for `Period`
module Patterns.Domain.Tests.PeriodsCarryingForward

open Patterns.Domain
open Patterns.Domain.Period
open FSharp.UMX
open Swensen.Unquote
open Xunit

[<Fact>]
let ``Happy path`` () =
    let store = Equinox.MemoryStore.VolatileStore() |> Store.Context.Memory
    let service = Factory.create store
    let decide items _state =
        let apply = Array.truncate 2 items
        let overflow = Array.skip apply.Length items
        (match overflow with [||] -> None | xs -> Some xs), // Apply max of two events
        (), // result
        [Events.Added {items = apply }]
    let add period events = service.Transact(PeriodId.parse period, decide, events) |> Async.RunSynchronously
    let read period = service.Read(PeriodId.parse period) |> Async.RunSynchronously
    add 0 [| %"a"; %"b" |]
    let expected = [| %"a"; %"b"|]
    test <@ Fold.Open expected = read 0 @>
    add 1 [| %"c"; %"d" |]
    let expected = [| %"a"; %"b"|], [| %"a"; %"b"|]
    test <@ Fold.Closed expected = read 0 @>
    let expected = [| %"a"; %"b"; %"c"; %"d" |]
    test <@ Fold.Open expected = read 1 @>
    let items period = read period |> Fold.(|Items|)
    add 1 [| %"e"; %"f"; %"g" |] // >2 items, therefore triggers an overflow
    let expected = [| %"a"; %"b"; %"c"; %"d"; %"e"; %"f" |]
    test <@ expected = items 1 @>
    let expected = [| %"a"; %"b"; %"c"; %"d"; %"e"; %"f"; %"g" |]
    test <@ expected = items 2 @>
    test <@ Fold.Initial = read 3 @>
    add 3 [| %"h" |]
    let expected = [| %"a"; %"b"; %"c"; %"d"; %"e"; %"f"; %"g"; %"h" |]
    test <@ Fold.Open expected = read 3 @>
