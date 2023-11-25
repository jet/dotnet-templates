module ArchiverTemplate.Handler

type Stats(log, statsInterval, stateInterval) =
    inherit Propulsion.Sync.Stats<unit>(log, statsInterval, stateInterval)

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

let selectArchivable changeFeedDocument: Propulsion.Sinks.StreamEvent seq = seq {
    for s, _e as se in Propulsion.CosmosStore.EquinoxSystemTextJsonParser.whereCategory categoryFilter changeFeedDocument do
        match FsCodec.StreamName.Category.ofStreamName s with
        | Archivable -> yield se
        | NotArchivable -> ()
}
