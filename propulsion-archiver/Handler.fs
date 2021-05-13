module ArchiverTemplate.Handler

type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Sync.Stats<unit>(log, statsInterval, stateInterval)

    override _.HandleOk(()) = ()
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

let (|Archivable|NotArchivable|) = function
    | "LokiPickTicketReservations"
    | "LokiDcBatch"
    | "LokiDcTransmissions" ->
        Archivable
    | _ ->
        NotArchivable

let selectArchivable (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    let s = changeFeedDocument.GetPropertyValue("p") |> string
    if s.StartsWith("events-") then () else
    for batch in Propulsion.CosmosStore.EquinoxCosmosStoreParser.enumStreamEvents changeFeedDocument do
        let (FsCodec.StreamName.CategoryAndId (cat,_)) = batch.stream
        match cat with
        | Archivable -> yield batch
        | NotArchivable -> ()
}
