namespace PeriodicIngesterTemplate.Domain

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guid+strings

type [<Measure>] ticketId
type TicketId = string<ticketId>
module TicketId =
    let toString (value: TicketId): string = %value
    let parse (value: string): TicketId = let raw = value in % raw
    let (|Parse|) = parse

/// Handles symmetric generation and decoding of StreamNames composed of a series of elements via the FsCodec.StreamId helpers
type internal CategoryId<'elements>(name, gen: 'elements -> FsCodec.StreamId, dec: FsCodec.StreamId -> 'elements) =
    member _.StreamName = gen >> FsCodec.StreamName.create name
    member _.TryDecode = FsCodec.StreamName.tryFind name >> ValueOption.map dec

[<RequireQualifiedAccess>]
type IngestionOutcome = Changed | Unchanged | Stale
