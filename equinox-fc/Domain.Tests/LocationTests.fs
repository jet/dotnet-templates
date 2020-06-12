module Fc.Domain.Tests.LocationTests

open FsCheck.Xunit
open FSharp.UMX
open Swensen.Unquote
open System

open Fc.Domain.Location

/// Helpers to match `module Cosmos/EventStore` wrapping inside the impl
module Location =

    module MemoryStore =

        open Equinox.MemoryStore

        module Series =

            let resolve store = Resolver(store, Series.Events.codec, Series.Fold.fold, Series.Fold.initial).Resolve

        module Epoch =

            let resolve store = Resolver(store, Epoch.Events.codec, Epoch.Fold.fold, Epoch.Fold.initial).Resolve

        let create (zeroBalance, toBalanceCarriedForward, shouldClose) store =
            let maxAttempts = Int32.MaxValue
            let series = Series.create (fun (id, _opt) -> Series.resolve store id) maxAttempts
            let epochs = Epoch.create (Epoch.resolve store) maxAttempts
            create (zeroBalance, toBalanceCarriedForward, shouldClose) (series, epochs)

let run (service : Fc.Domain.Location.Service) (IdsAtLeastOne locations, deltas : _[], transactionId) = Async.RunSynchronously <| async {
    let runId = mkId () // Need to make making state in store unique when replaying or shrinking
    let locations = locations |> Array.map (fun x -> % (sprintf "%O/%O" x runId))

    let updates = deltas |> Seq.mapi (fun i x -> locations.[i % locations.Length], x) |> Seq.cache

    (* Apply random deltas *)

    let adjust delta (state : Epoch.Fold.State) =
        let (Epoch.Fold.Balance bal) = state
        let value = max -bal delta
        if value = 0 then 0, []
        elif value < 0 then value, [Epoch.Events.Removed {| delta = -value; id = transactionId |}]
        else value, [Epoch.Events.Added {| delta = value; id = transactionId |}]
    let! appliedDeltas = seq { for loc, x in updates -> async { let! eff = service.Execute(loc, adjust x) in return loc,eff } } |> Async.Parallel
    let expectedBalances = Seq.append (seq { for l in locations -> l, 0}) appliedDeltas |> Seq.groupBy fst |> Seq.map (fun (l, xs) -> l, xs |> Seq.sumBy snd) |> Set.ofSeq

    (* Verify loading yields identical state *)

    let! balances = seq { for loc in locations -> async { let! bal = service.Execute(loc,(fun (Epoch.Fold.Balance bal) -> bal, [])) in return loc,bal } } |> Async.Parallel
    test <@ expectedBalances = Set.ofSeq balances @> }

let [<Property>] ``MemoryStore properties`` epochLen args =
    let store = Equinox.MemoryStore.VolatileStore()

    let epochLen, idsWindow = max 1 epochLen, 5
    let zero, cf, sc = Epoch.zeroBalance, Epoch.toBalanceCarriedForward idsWindow, Epoch.shouldClose epochLen

    let service = Location.MemoryStore.create (zero, cf, sc) store
    run service args

type EventStore(testOutput) =

    let log = TestOutputLogger.create testOutput
    do Serilog.Log.Logger <- log

    let context, cache = EventStore.connect ()

    let [<Property(MaxTest=5, MaxFail=1)>] properties epochLen args =
        let epochLen, idsWindow = max 1 epochLen, 5
        let zero, cf, sc = Epoch.zeroBalance, Epoch.toBalanceCarriedForward idsWindow, Epoch.shouldClose epochLen

        let service = Fc.Domain.Location.EventStore.create (zero, cf, sc) (context, cache, 50)
        run service args
