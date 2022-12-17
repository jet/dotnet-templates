module Domain.Tests.GroupCheckoutProcess

open FsCheck.Xunit
open Swensen.Unquote

open Domain.GroupCheckoutProcess

[<Property>]
let ``Adding stays should trigger Checkout Action`` (sut : Service) id stays = async {
    let! act = sut.Add(id, stays)
    test <@ match act with
            | Flow.Checkout staysToDo -> staysToDo = stays
            | Flow.Ready 0m -> [||] = stays
            | _ -> false @> }
