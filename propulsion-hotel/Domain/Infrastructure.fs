namespace global

module Guid =
    let gen () = System.Guid.NewGuid()
    let parse: string -> System.Guid = System.Guid.Parse
    let toStringN (x: System.Guid): string = x.ToString "N"

type DateTimeOffset = System.DateTimeOffset
type HashSet<'t> = System.Collections.Generic.HashSet<'t>

/// Handles symmetric generation and decoding of StreamNames composed of a series of elements via the FsCodec.StreamId helpers
type internal CategoryId<'elements>(name, gen: 'elements -> FsCodec.StreamId, dec: FsCodec.StreamId -> 'elements) =
    member _.StreamName = gen >> FsCodec.StreamName.create name
    member _.TryDecode = FsCodec.StreamName.tryFind name >> ValueOption.map dec

[<AutoOpen>]
module DeciderExtensions =
 
    type Equinox.Decider<'E, 'S> with

        member x.TransactWithPostVersion(decide: 'S -> Async<'R * 'E[]>): Async<'R * int64> =
            x.TransactEx((fun c -> decide c.State),
                         (fun r (c: Equinox.ISyncContext<'S>) -> (r, c.Version)))
