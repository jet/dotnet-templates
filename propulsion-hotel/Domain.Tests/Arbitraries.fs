[<Microsoft.FSharp.Core.AutoOpen>]
module Domain.Tests.Arbitraries

open Domain
open FsCheck.FSharp

type Generators =

    static member MemoryStore = Gen.constant (Config.Store.Memory <| Equinox.MemoryStore.VolatileStore())
    static member Store = Arb.fromGen Generators.MemoryStore

[<assembly: FsCheck.Xunit.Properties(Arbitrary = [| typeof<Generators> |])>] do ()
