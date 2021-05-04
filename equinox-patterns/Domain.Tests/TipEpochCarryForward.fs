/// This test and implementation pairing demonstrates how one might accomplish a pattern
module Patterns.Domain.Tests.TipEpochCarryForward

open Patterns.Domain
open Patterns.Domain.Epoch
open Swensen.Unquote
open Xunit

[<Fact>]
let ``Happy path`` () =
    let store = Equinox.MemoryStore.VolatileStore()
    let service = MemoryStore.create store
    let go = Async.RunSynchronously
    let add epoch events = service.Transact(EpochId.parse epoch, (fun es _state -> (), es), events) |> go
    let read epoch = service.Read(EpochId.parse epoch) |> go
    add 0 [Events.Added {items = [| "a"; "b" |]}]
    test <@ Fold.Open [|"a"; "b"|] = read 0 @>
    add 1 [Events.Added {items = [| "c"; "d" |]}]
    test <@ Fold.Closed ([|"a"; "b"|], [|"a"; "b"|]) = read 0 @>
    test <@ Fold.Open [|"a"; "b"; "c"; "d" |] = read 1 @>
    add 0 [Events.Added {items = [| "e" |]}]
    test <@ Fold.Open [|"a"; "b"; "c"; "d"; "e" |] = read 1 @>
