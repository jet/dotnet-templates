module ArchiverTemplate.Handler

type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Sync.Stats<unit>(log, statsInterval, stateInterval)

    override __.HandleOk(()) = ()
    override __.HandleExn exn = log.Information(exn, "Unhandled")

let transformOrFilter (changeFeedDocument: Microsoft.Azure.Documents.Document) : Propulsion.Streams.StreamEvent<_> seq = seq {
    for batch in Propulsion.Cosmos.EquinoxCosmosParser.enumStreamEvents changeFeedDocument do
        match batch.stream with
        // TODO whitelist/blacklist to ensure only relevant streams get archived
        | FsCodec.StreamName.CategoryAndId (_, _) -> yield batch
}
