module ArchiverTemplate.Handler

type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Streams.Sync.Stats<unit>(log, statsInterval, stateInterval)

    override _.HandleOk(()) = ()
    override _.HandleExn(log, exn) =
        log.Information(exn, "Unhandled")

let categoryFilter = function
    | "CategoryName" -> true
    | _ -> false
    
let (|Archivable|NotArchivable|) = function
    // TODO define Categories that should be copied to the secondary Container
    | "CategoryName" ->
        Archivable
    | _ ->
        NotArchivable

let selectArchivable changeFeedDocument: Propulsion.Streams.StreamEvent<_> seq = seq {
    for struct (s, _e) as batch in Propulsion.CosmosStore.EquinoxSystemTextJsonParser.enumStreamEvents categoryFilter changeFeedDocument do
        let (FsCodec.StreamName.Category cat) = s
        match cat with
        | Archivable -> yield batch
        | NotArchivable -> ()
}
