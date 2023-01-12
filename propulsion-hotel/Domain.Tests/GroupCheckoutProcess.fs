module Domain.Tests.GroupCheckoutProcess

open Domain.GroupCheckout
open FsCheck.Xunit

[<Property>]
let ``Merging stays should trigger Checkout Reaction`` (sut : Service) id stays = async {
    let! act = sut.Merge(id, stays)
    return act |> function
        // If any stays have been added, they should be recorded
        | Flow.Checkout staysToDo -> staysToDo = stays
        // If no stays were added to the group checkout, no checkouts actions should be pending
        | Flow.Ready 0m -> [||] = stays
        // We should not end up in any other states
        | Flow.Ready _
        | Flow.Finished -> false }
