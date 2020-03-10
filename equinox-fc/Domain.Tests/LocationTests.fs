module Fc.LocationTests

open Fc.Location
open FsCheck.Xunit
open FSharp.UMX
open Swensen.Unquote
open System

/// Helpers to match `module Cosmos` wrapping inside the impl
module Location =

    module MemoryStore =

        open Equinox.MemoryStore

        module Series =

            let resolver store = Resolver(store, Series.Events.codec, Series.Fold.fold, Series.Fold.initial).Resolve

        module Epoch =

            let resolver store = Resolver(store, Epoch.Events.codec, Epoch.Fold.fold, Epoch.Fold.initial).Resolve

        let createService (zeroBalance, shouldClose) store =
            let maxAttempts = Int32.MaxValue
            let series = Series.create (Series.resolver store) maxAttempts
            let epochs = Epoch.create (Epoch.resolver store) maxAttempts
            create (zeroBalance, shouldClose) (series, epochs)

let run (service : Location.Service) (IdsAtLeastOne locations, deltas : _[], transactionId) = Async.RunSynchronously <| async {
    let runId = mkId () // Need to make making state in store unique when replaying or shrinking
    let locations = locations |> Array.map (fun x -> % (sprintf "%O/%O" x runId))

    let updates = deltas |> Seq.mapi (fun i x -> locations.[i % locations.Length], x) |> Seq.cache

    (* Apply random deltas *)

    let adjust delta (bal : Epoch.Fold.Balance) =
        let value = max -bal delta
        if value = 0 then 0, []
        elif value < 0 then value, [Epoch.Events.Removed {| delta = -value; id = transactionId |}]
        else value, [Epoch.Events.Added {| delta = value; id = transactionId |}]
    let! appliedDeltas = seq { for loc, x in updates -> async { let! _, eff = service.Execute(loc, adjust x) in return loc,eff } } |> Async.Parallel
    let expectedBalances = Seq.append (seq { for l in locations -> l, 0}) appliedDeltas |> Seq.groupBy fst |> Seq.map (fun (l, xs) -> l, xs |> Seq.sumBy snd) |> Set.ofSeq

    (* Verify loading yields identical state *)

    let! balances = seq { for loc in locations -> async { let! bal, () = service.Execute(loc,(fun _ -> (), [])) in return loc,bal } } |> Async.Parallel
    test <@ expectedBalances = Set.ofSeq balances @> }

let [<Property>] ``MemoryStore properties`` maxEvents args =
    let store = Equinox.MemoryStore.VolatileStore()
    let zeroBalance = 0
    let maxEvents = max 1 maxEvents
    let shouldClose (state : Epoch.Fold.OpenState) = state.count > maxEvents
    let service = Location.MemoryStore.createService (zeroBalance, shouldClose) store
    run service args

type Cosmos(testOutput) =

    let context, cache = Cosmos.connect ()

    let log = testOutput |> TestOutputAdapter |> createLogger
    do Serilog.Log.Logger <- log

    let [<Property(MaxTest=10)>] properties maxEvents args =
        let zeroBalance = 0
        let maxEvents = max 1 maxEvents
        let shouldClose (state : Epoch.Fold.OpenState) = state.count > maxEvents
        let service = Location.Cosmos.createService (zeroBalance, shouldClose) (context, cache, 50)
        run service args
