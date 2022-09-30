module Shipping.Watchdog.Lambda.Cdk.Program

open Amazon.CDK

let topicNameArg = "notifyTopicArn"
let indexTableNameArg = "indexTableName"
let storeTableNameArg = "storeTableName"
let codePathArg = "code"

[<EntryPoint>]
let main _ =
    let app = App(null)

    // TODO probably replace with Logic to look it up in SSM Parameter Store
    let codePath, topicArn =
        match app.Node.TryGetContext codePathArg, app.Node.TryGetContext topicNameArg with
        | :? string as sa, (:? string as tn) when sa <> null && tn <> null -> sa, tn
        | _ -> failwith $"Please supply -c {topicNameArg}=<SNS Topic Arn> -c {indexTableNameArg}=<Index Table Name> -c {storeTableNameArg}=<Store Table Name> and -c {codePathArg}=<Code Path>"
    let indexTableName = "equinox-test-index"
    let storeTableName = "equinox-test"
    let _watchdogLambda = WatchdogLambdaStack(app, "Watchdog", WatchdogLambdaStackProps(
        topicArn, indexTableName, storeTableName,
        codePath, "Shipping Watchdog", "Watchdog.Lambda::Shipping.Watchdog.Lambda.Function::Handle"))

    app.Synth() |> ignore
    0
