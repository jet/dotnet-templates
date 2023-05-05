namespace Shipping.Domain.Tests

open Propulsion.MemoryStore
open System

// TODO remove after Propulsion 3rc4 
module MemoryStoreLogger =
    open System.Threading
    let subscribe log source =
        let mutable epoch = -1L
        let aux struct (sn, events) =
            let struct (categoryName, streamId) = FsCodec.StreamName.splitCategoryAndStreamId sn
            let epoch = Interlocked.Increment &epoch
            MemoryStoreLogger.renderSubmit log (epoch, categoryName, streamId, events)
        if log.IsEnabled Serilog.Events.LogEventLevel.Debug then Observable.subscribe aux source
        else { new IDisposable with member _.Dispose() = () }

/// Holds Equinox MemoryStore. Disposable to correctly manage unsubscription of logger at end of test
type MemoryStoreFixture() =
    let store = Equinox.MemoryStore.VolatileStore<struct (int * ReadOnlyMemory<byte>)>()
    let mutable disconnectLog: (unit -> unit) option = None
    member val Config = Shipping.Domain.Store.Context.Memory store
    member _.Committed = store.Committed
    member _.TestOutput with set testOutput =
        if Option.isSome disconnectLog then invalidOp "Cannot connect more than one test output"
        let log = XunitLogger.forTest testOutput
        disconnectLog <- Some (MemoryStoreLogger.subscribe log store.Committed).Dispose
    interface IDisposable with member _.Dispose() = match disconnectLog with Some f -> f () | None -> ()
