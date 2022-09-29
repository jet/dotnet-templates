module DynamoStoreCdkTemplate.Program

open Amazon.CDK

// See logic in .fsproj file, which copies content from `Propulsion.DynamoStore.Indexer`'s tools/ folder to this well known location
let lambdaCodePath = "obj/pub/Indexer/net6.0/linux-arm64/"

(* Indexer-related *)
let streamNameArg = "streamArn"
let indexTableNameArg = "indexTableName"

(* Notifier-related *)
let indexStreamArg = "indexStreamArn"
let topicNameArg = "notifyTopicArn"

[<EntryPoint>]
let main _ =
    let app = App(null)

    // TODO probably replace with Logic to look it up in SSM Parameter Store
    let storeStreamArn, indexTableName =
        match app.Node.TryGetContext streamNameArg, app.Node.TryGetContext indexTableNameArg with
        | :? string as sa, (:? string as tn) when sa <> null && tn <> null -> sa, tn
        | _ -> failwith $"Please supply DynamoDB Streams ARN and DynamoDB Index Table Name via -c {streamNameArg}= and -c {indexTableNameArg}= respectively"
    let indexerCodePath = match app.Node.TryGetContext "indexerCode" with :? string as path -> path | _ -> lambdaCodePath + "/Indexer"

    let _mainIndexer = IndexerStack(app, "MainIndexer", IndexerStackProps(storeStreamArn, indexTableName, indexerCodePath))
    // TOCONSIDER - if you have >1 table for your application, instantiate additional IndexerStacks here
    
    // NOTE the Notifier Lambda is an optional component, only of relevance when you're utilizing Lambda Reactors
    // (the more common arrangement is to have a long-running host application poll direct from the index using a DynamoStoreSource)
    let notifierCodePath = match app.Node.TryGetContext "notifierCode" with :? string as path -> path | _ -> lambdaCodePath + "/Notifier"
    let indexStreamArn, notifyTopicArn =
        match app.Node.TryGetContext indexStreamArg, app.Node.TryGetContext topicNameArg with
        | :? string as sa, (:? string as tn) when sa <> null -> sa, Option.ofObj tn
        | _ -> failwith $"Please supply DynamoDB Index Streams ARN and (optionally) SNS Topic ARN via -c {indexStreamArg}= and -c {topicNameArg}= respectively"
    let _mainNotifier = NotifierStack(app, "MainNotifier", NotifierStackProps(indexStreamArn, notifyTopicArn, notifierCodePath))

    app.Synth() |> ignore
    0
