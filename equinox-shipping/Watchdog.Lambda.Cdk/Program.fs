module Shipping.Watchdog.Lambda.Cdk.Program

let topicNameArg = "notifyTopicArn"
let indexTableNameArg = "indexTableName"
let storeTableNameArg = "storeTableName"
let codePathArg = "code"

[<EntryPoint>]
let main _ =
    let app = Amazon.CDK.App(null)

    let getArg = app.Node.TryGetContext // TODO probably replace with Logic to look it up in SSM Parameter Store
    let codePath, topicArn, storeTableName, indexTableName =
        match getArg codePathArg, getArg topicNameArg, getArg storeTableNameArg, getArg indexTableNameArg with
        | :? string as sa, (:? string as tn), (:? string as st), (:? string as it)
            when sa <> null && tn <> null && st <> null && it <> null -> sa, tn, st, it
        | _ -> failwith $"Please supply -c %s{topicNameArg}=<SNS Topic Arn>
                          -c %s{indexTableNameArg}=<Index Table Name> -c %s{storeTableNameArg}=<Store Table Name>
                          and -c %s{codePathArg}=<Code Path>"
    let _watchdogLambda = WatchdogLambdaStack(app, "Watchdog", WatchdogLambdaStackProps(
        topicArn, indexTableName, storeTableName,
        codePath, "Shipping Watchdog", "Watchdog.Lambda::Shipping.Watchdog.Lambda.Function::Handle"))

    app.Synth() |> ignore
    0
