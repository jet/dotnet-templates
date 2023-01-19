/// Commandline arguments and/or secrets loading specifications
module Infrastructure.Args

module Config = Domain.Config

exception MissingArg of message : string with override this.Message = this.message
let missingArg msg = raise (MissingArg msg)

module Configuration =

    module Dynamo =

        let [<Literal>] REGION =            "EQUINOX_DYNAMO_REGION"
        let [<Literal>] SERVICE_URL =       "EQUINOX_DYNAMO_SERVICE_URL"
        let [<Literal>] ACCESS_KEY =        "EQUINOX_DYNAMO_ACCESS_KEY_ID"
        let [<Literal>] SECRET_KEY =        "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
        let [<Literal>] TABLE =             "EQUINOX_DYNAMO_TABLE"
        let [<Literal>] INDEX_TABLE =       "EQUINOX_DYNAMO_TABLE_INDEX"
        
    module Mdb =
        
        let [<Literal>] CONNECTION_STRING = "MDB_CONNECTION_STRING"
        let [<Literal>] READ_CONN_STRING =  "MDB_CONNECTION_STRING_READ"
        let [<Literal>] SCHEMA =            "MDB_SCHEMA"

type Configuration(tryGet : string -> string option) =

    member val tryGet =                     tryGet
    member _.get key =                      match tryGet key with Some value -> value | None -> missingArg $"Missing Argument/Environment Variable %s{key}"


    member x.DynamoRegion =                 tryGet Configuration.Dynamo.REGION
    member x.DynamoServiceUrl =             x.get Configuration.Dynamo.SERVICE_URL
    member x.DynamoAccessKey =              x.get Configuration.Dynamo.ACCESS_KEY
    member x.DynamoSecretKey =              x.get Configuration.Dynamo.SECRET_KEY
    member x.DynamoTable =                  x.get Configuration.Dynamo.TABLE
    member x.DynamoIndexTable =             tryGet Configuration.Dynamo.INDEX_TABLE

    member x.MdbConnectionString =          x.get Configuration.Mdb.CONNECTION_STRING
    member x.MdbReadConnectionString =      tryGet Configuration.Mdb.READ_CONN_STRING
    member x.MdbSchema =                    x.get Configuration.Mdb.SCHEMA
    
    member x.PrometheusPort =               tryGet "PROMETHEUS_PORT" |> Option.map int
