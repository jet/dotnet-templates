module TestbedTemplate.Tests

open Domain
open FSharp.UMX
open Infrastructure
open Microsoft.Extensions.DependencyInjection
open System
open System.Net.Http
open System.Text

type Test = Favorite

let mkSkuId () = Guid.NewGuid() |> SkuId

let executeLocal (container: ServiceProvider) test: ClientId -> Async<unit> =
    match test with
    | Favorite ->
        let service = container.GetRequiredService<Backend.Favorites.Service>()
        fun clientId -> async {
            let sku = mkSkuId ()
            do! service.Favorite(clientId,[sku])
            let! items = service.List clientId
            if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
