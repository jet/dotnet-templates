[<Microsoft.FSharp.Core.AutoOpen>]
module Domain.Tests.Arbitraries

open FsCheck
open FsCheck.FSharp

open Domain

let genDefault<'t> = ArbMap.defaults |> ArbMap.generate<'t>

type Generators =

    static member MemoryStore = Gen.constant (Config.Store.Memory <| Equinox.MemoryStore.VolatileStore())
    static member Store = Arb.fromGen Generators.MemoryStore

    static member GroupCheckoutProcess : Arbitrary<GroupCheckoutProcess.Service> =
        Generators.MemoryStore |> Gen.map GroupCheckoutProcess.Config.create |> Arb.fromGen

[<assembly: FsCheck.Xunit.Properties(Arbitrary = [| typeof<Generators> |])>] do ()
