[<AutoOpen>]
module Shipping.Domain.Tests.Fixtures

open Shipping.Domain

module FinalizationTransaction =
    open FinalizationTransaction
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let cat = MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
            create cat.Resolve
module Container =
    open Container
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let cat = MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
            create cat.Resolve
module Shipment =
    open Shipment
    module MemoryStore =
        open Equinox.MemoryStore
        let create store =
            let cat = MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
            create cat.Resolve

let createProcessManager maxDop store =
    let transactions = FinalizationTransaction.MemoryStore.create store
    let containers = Container.MemoryStore.create store
    let shipments = Shipment.MemoryStore.create store
    FinalizationProcessManager.Service(transactions, containers, shipments, maxDop=maxDop)

(* Generic FsCheck helpers *)

let (|Id|) (x : System.Guid) = x.ToString "N" |> FSharp.UMX.UMX.tag
let (|Ids|) (xs : System.Guid[]) = xs |> Array.map (|Id|)
let (|IdsAtLeastOne|) (Ids xs, Id x) = [| yield x; yield! xs |]
let (|AtLeastOne|) (x, xs) = x::xs
