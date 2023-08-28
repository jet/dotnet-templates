module App

exception MissingArg of message: string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

type Configuration(tryGet: string -> string option) =

    let get key = match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get "EQUINOX_COSMOS_CONNECTION"
    member _.CosmosDatabase =               get "EQUINOX_COSMOS_DATABASE"
    member _.CosmosContainer =              get "EQUINOX_COSMOS_CONTAINER"

