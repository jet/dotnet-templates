/// Integration suite for `Period`
module Patterns.Domain.Tests.PeriodsCarryingForward

open Patterns.Domain
open Patterns.Domain.Period
open Swensen.Unquote
open Xunit

[<Fact>]
let ``Happy path`` () =
    let store = Equinox.MemoryStore.VolatileStore()
    let service = MemoryStore.create store
    let decide items _state =
        let apply = Array.truncate 2 items
        let overflow = Array.skip apply.Length items
        (match overflow with [||] -> None | xs -> Some xs), // Apply max of two events
        (), // result
        [Events.Added {items = apply }]
    let add period events = service.Transact(PeriodId.parse period, decide, events) |> Async.RunSynchronously
    let read period = service.Read(PeriodId.parse period) |> Async.RunSynchronously
    add 0 [| "a"; "b" |]
    test <@ Fold.Open [|"a"; "b"|] = read 0 @>
    add 1 [| "c"; "d" |]
    test <@ Fold.Closed ([|"a"; "b"|], [|"a"; "b"|]) = read 0 @>
    test <@ Fold.Open [|"a"; "b"; "c"; "d" |] = read 1 @>
    let items period = read period |> Fold.(|Items|)
    add 1 [| "e"; "f"; "g" |] // >2 items, therefore triggers an overflow
    test <@ [|"a"; "b"; "c"; "d"; "e"; "f" |] = items 1 @>
    test <@ [|"a"; "b"; "c"; "d"; "e"; "f"; "g" |] = items 2 @>
    test <@ Fold.Initial = read 3 @>
    add 3 [| "h" |]
    test <@ Fold.Open [|"a"; "b"; "c"; "d"; "e"; "f"; "g"; "h" |] = read 3 @>
