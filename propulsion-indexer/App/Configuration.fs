module App.Args

exception MissingArg of message: string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

let [<Literal>] CONNECTION =                "EQUINOX_COSMOS_CONNECTION"
let [<Literal>] DATABASE =                  "EQUINOX_COSMOS_DATABASE"
let [<Literal>] CONTAINER =                 "EQUINOX_COSMOS_CONTAINER"
let [<Literal>] VIEWS =                     "EQUINOX_COSMOS_VIEWS"

type Configuration(tryGet: string -> string option) =

    let get key = match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"

    member _.CosmosConnection =             get CONNECTION
    member _.CosmosDatabase =               get DATABASE
    member _.CosmosContainer =              get CONTAINER
    member _.CosmosViews =                  get VIEWS

