module TestbedTemplate.Tests

open FSharp.UMX
open Microsoft.Extensions.DependencyInjection
open System

type Test = Favorite

let mkSkuId () = % Guid.NewGuid()

let executeLocal (container: ServiceProvider) test: ClientId -> Async<unit> =
    match test with
    | Favorite ->
        let service = container.GetRequiredService<Services.Domain.Favorites.Service>()
        fun clientId -> async {
            let sku = mkSkuId ()
            do! service.Favorite(clientId, [sku])
            let! items = service.List clientId
            if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }