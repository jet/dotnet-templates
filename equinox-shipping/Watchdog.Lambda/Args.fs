// TODO delete after Propulsion 3b4.3
module Propulsion.DynamoStore.Lambda.Args

#if PROPULSION_DYNAMOSTORE_LAMBDA
module Dynamo =
#else
module internal Dynamo =
#endif

    // let [<Literal>] REGION =                    "EQUINOX_DYNAMO_REGION"
    // hacked to match old value in constructs
    let [<Literal>] REGION =                    "EQUINOX_DYNAMO_SYSTEM_NAME"
    let [<Literal>] SERVICE_URL =               "EQUINOX_DYNAMO_SERVICE_URL"
    let [<Literal>] ACCESS_KEY =                "EQUINOX_DYNAMO_ACCESS_KEY_ID"
    let [<Literal>] SECRET_KEY =                "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
    let [<Literal>] TABLE =                     "EQUINOX_DYNAMO_TABLE"
    let [<Literal>] INDEX_TABLE =               "EQUINOX_DYNAMO_TABLE_INDEX"
