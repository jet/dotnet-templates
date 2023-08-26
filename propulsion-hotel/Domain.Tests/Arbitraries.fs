[<Microsoft.FSharp.Core.AutoOpen>]
module Domain.Tests.Arbitraries

open Domain
open FsCheck.FSharp

/// For unit tests, we only ever use the Domain Services wired to a MemoryStore, so we default Store to that
type Generators =

    static member MemoryStore = Gen.constant (Store.Config.Memory <| Equinox.MemoryStore.VolatileStore())
    static member Store = Arb.fromGen Generators.MemoryStore

[<assembly: FsCheck.Xunit.Properties(Arbitrary = [| typeof<Generators> |])>] do ()
